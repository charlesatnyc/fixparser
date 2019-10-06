package com.xdh.app.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.xdh.app.Message;
import com.xdh.app.MessageListener;
import com.xdh.app.exp.FIXParsingException;

/**
 * FXParser for repeating group(s)
 * 
 * @author haihuang (charlesatnyc@gmail.com)
 *
 */
public class FIXParser<T> implements InitializingBean, DisposableBean, MessageListener<T> {

	private static final Logger logger = LogManager.getLogger(FIXParser.class);
	private String repeatingGroups;
	private int concurrentLevel;
	private ExecutorService es;
	private ExecutorService errorHandlingService;
	private BlockingQueue<String> errorHandlingQueue;
	private String msgEncoding;
	private final Map<String, List<String>> repeatingGrpIndToFirstRequired = new HashMap<>();
	private final List<String> topLevelRepeatingGroupInd = new ArrayList<>();
	private final Map<String, List<String>> repeatingGrpIndToFirstOptional = new HashMap<>();
	private final ConcurrentMap<Long, AtomicReference<Map<String, Node>>> msgIdToMsgNode = new ConcurrentHashMap<>();
	private final static String defaultEncoding = "UTF-8";

	public FIXParser() {
	}

	/**
	 * the data structure which represent repeating group(s)
	 *
	 */
	static class Node {
		String tag;
		String val;
		Node[] children;
		Node() {
		}

		Node(String tagNumber, String value, int buckets) {
			tag = tagNumber;
			val = value;
			children = new Node[buckets];
		}

		Node(String tagNumber, String value) {
			tag = tagNumber;
			val = value;
		}

		@Override
		public String toString() {
			StringBuilder sb = new StringBuilder();
			sb.append(tag).append("=");
			sb.append(val);
			if (children != null && children.length > 0) {
				sb.append("->[");
				for (Node child : children) {
					if (child != null)
						sb.append(child.toString()).append("|");
				}
				if (sb.charAt(sb.length()-1) == '|') {
					sb.deleteCharAt(sb.length()-1);
				}
				sb.append("]");
			} 
		
			return sb.toString();
		}
	}

	/**
	 * check if the first tag within a repeating group is valid
	 * 
	 * @param rootTag  the root tag of repeating group
	 * @param firstTag the first tag within a repeating group
	 * @return true if valid; otherwise false;
	 */
	boolean checkFirstTagInRepeatingGroup(String rootTag, String firstTag) {
		List<String> required = repeatingGrpIndToFirstRequired.getOrDefault(rootTag, new ArrayList<String>());
		return required.get(0) == firstTag;
	}

	/**
	 * process repeating groups
	 * 
	 * @param tagValuePairs the pair of tag/value array in the format of tag=value
	 * @param start         the index which repeating group starts at
	 * @param required      the list of required tags within repeating group
	 * @param optionals     the list of required and optional tags within repeating
	 *                      group
	 * @param results       the map which contains parsed tag/value pair
	 * @return the root node for repeating group; null if failing.
	 */
	Node processRepeatingGroups(String[] tagValuePairs, int[] start, List<String> required, List<String> optionals,
			Map<String, Node> results) {
		String[] indKeyValue = tagValuePairs[start[0]].split("=");
		String indTag = indKeyValue[0];
		if (results.containsKey(indTag) && !repeatingGrpIndToFirstRequired.containsKey(indTag)) {
			logger.error("duplicate tag {}", indTag);
			return null;
		}
		Integer numOfGroups = Integer.valueOf(indKeyValue[1]);
		int countOfGroups = numOfGroups.intValue();
		Node current = new Node(indTag, indKeyValue[1], numOfGroups);
		int idx = 0;
		boolean exit = false;
		int countOfRequired = required.size();
		int countOfOptional = optionals.size();
		int totalCountOfGroupTags = countOfRequired + countOfOptional;
		int i = start[0] + 1;
		for (; i < tagValuePairs.length && countOfGroups > 0; i++) {
			countOfRequired = required.size();
			String[] firstKeyValue = tagValuePairs[i].split("=");
			if (checkFirstTagInRepeatingGroup(indTag, firstKeyValue[0])) {
				logger.error("fixFirst tag is not valid. {}", (Object[]) tagValuePairs);
				return null;
			}
			countOfRequired--;
			Node firstTagNode = new Node(firstKeyValue[0], firstKeyValue[1], totalCountOfGroupTags);
			current.children[idx] = firstTagNode;
			idx++;
			int j = i + 1;
			int idxForChildren = 0;
			for (; j < tagValuePairs.length; j++) {
				String[] keyValue = tagValuePairs[j].split("=");
				if (firstKeyValue[0].equals(keyValue[0])) {
					countOfGroups--;
					i = j;
					break;
				}

				if (!optionals.contains(keyValue[0]) && !required.contains(keyValue[0])) {
					if (idxForChildren > totalCountOfGroupTags || countOfRequired > 0) {
						logger.error("message is in bad format - {}", keyValue[0]);
						return null;
					}
					i = j - 1;
					exit = true;
					break;
				}
				if (required.contains(keyValue[0])) {
					countOfRequired--;
				}
				Node tagNode;
				// process embedded repeatingGroups
				if (repeatingGrpIndToFirstRequired.containsKey(keyValue[0])) {
					List<String> subReqs = repeatingGrpIndToFirstRequired.get(keyValue[0]);
					List<String> subOpts = repeatingGrpIndToFirstOptional.get(keyValue[0]);
					start[0] = j;
					tagNode = processRepeatingGroups(tagValuePairs, start, subReqs, subOpts, results);
					if (firstTagNode.children[idxForChildren] != null) {
						logger.error("message is in bad format. found duplicate - {}",
								firstTagNode.children[idxForChildren]);
						return null;
					}
					firstTagNode.children[idxForChildren++] = tagNode;
					j = start[0];
					continue;
				}
				//

				tagNode = new Node(keyValue[0], keyValue[1]);
				if (firstTagNode.children[idxForChildren] != null) {
					logger.error("message is in bad format. found duplicate - {}",
							firstTagNode.children[idxForChildren]);
					return null;
				}
				firstTagNode.children[idxForChildren++] = tagNode;
			}
			if (exit) {
				break;
			}
			i = j - 1;
		}
		start[0] = i;
		if (!results.containsKey(indTag)) {
		//	results.get(indTag).sibling = current;
		//} else {
			results.put(indTag, current);
		}
		return current;
	}

	/**
	 * error handler which processes failed fix messages
	 * 
	 */
	class ErrorHandler implements Runnable {
		private final BlockingQueue<String> errorQueue;
		private volatile boolean stop = false;
		private AtomicLong processed = new AtomicLong(0);

		/**
		 * constructor of ErrorHandler
		 * 
		 * @param errQueue error handling queue
		 */
		ErrorHandler(BlockingQueue<String> errQueue) {
			errorQueue = errQueue;
		}

		@Override
		public void run() {
			try {
				while (!stop) {
					if (errorQueue.isEmpty()) {
						Thread.sleep(1000);
					} else {
						String failedFixMsg = errorQueue.take();
						process(failedFixMsg);
					}
				}
			} catch (Exception e) {
				logger.error("error_handler failed. {}", e);
			} finally {
				logger.error("[error_handling] number of failed FIX message not processed: {]", errorQueue.size());
			}
		}

		/**
		 * stop the error handling work
		 * 
		 */
		public void stop() {
			stop = true;
		}

		/**
		 * further process failed fix messages
		 * 
		 * @param failedFixMsg failed fix message
		 */
		private void process(String failedFixMsg) {
			long numOfProcessed = processed.incrementAndGet();
			logger.error("[error_handling] # {} - failed FIX message: {]", numOfProcessed, failedFixMsg);
			// TODO: apply error handle policy here

		}

		/**
		 * get the number of processed failed fix messages
		 * 
		 * @return the number of processed failed fix messages
		 */
		public long getStatistics() {
			return processed.get();
		}
	}

	/**
	 * worker thread to process a fix message
	 * 
	 *
	 */
	class Worker implements Runnable {
		private final String fixMessage;
		private final Long msgId;
		private final BlockingQueue<String> errorQueue;

		/**
		 * constructor of Worker
		 * 
		 * @param messageId message id
		 * @param fixMsg    fix message in String format
		 * @param errQueue  error handling queue
		 */
		Worker(Long messageId, String fixMsg, BlockingQueue<String> errQueue) {
			msgId = messageId;
			fixMessage = fixMsg;
			errorQueue = errQueue;
		}

		@Override
		public void run() {
			try {
				parseMessage();
			} catch (Exception e) {
				logger.error("failed to parse incoming message - id::{}, error::{}", msgId, e);
				try {
					boolean addError = errorQueue.offer(fixMessage, 1000, TimeUnit.MILLISECONDS);
					if (!addError) {
						logger.error("failed to add failed fix message to error queue - id::{}, fix::{}", msgId,
								fixMessage);
					}
				} catch (InterruptedException e1) {
					logger.error("failed to add failed fix message to error queue - id::{}, fix::{}", msgId,
							fixMessage);
				}
			}

		}

		private void parseMessage() throws Exception {
			Map<String, Node> results = new LinkedHashMap<>();
			String[] tagValuePair = fixMessage.split("\\|");
			for (int i = 0; i < tagValuePair.length; i++) {
				String[] keyValue = tagValuePair[i].split("=");
				Node tagNode;
				List<String> requiredTags = repeatingGrpIndToFirstRequired.get(keyValue[0]);
				if (requiredTags != null) {
					List<String> optionalTags = repeatingGrpIndToFirstOptional.get(keyValue[0]);
					int[] start = new int[] { i };
					tagNode = processRepeatingGroups(tagValuePair, start, requiredTags, optionalTags, results);
					if (tagNode == null) {
						throw new FIXParsingException("failed to parse repeating group of message::" + fixMessage
								+ " repeatingGroupIndicator::" + keyValue[0]);
					}
					i = start[0];
				} else {
					tagNode = new Node(keyValue[0], keyValue[1]);
				}
				results.put(keyValue[0], tagNode);
			}

			AtomicReference<Map<String, Node>> nodeRef = msgIdToMsgNode.get(msgId);
			if (nodeRef == null) {
				msgIdToMsgNode.putIfAbsent(msgId, new AtomicReference<Map<String, Node>>());
			}
			nodeRef = msgIdToMsgNode.get(msgId);
			if (nodeRef.get() == null) {
				synchronized (nodeRef) {
					if (nodeRef.get() == null) {
						nodeRef.set(results);
					} else {
						logger.error("duplicate incoming message - id::{}", msgId);
						return;
					}
				}
			} else {
				logger.error("duplicate incoming message - id::{}", msgId);
				return;
			}

		}
	}

	public void setRepeatingGroups(String repeatingGroups) {
		this.repeatingGroups = repeatingGroups;
	}

	public void setConcurrentLevel(int concurrentLevel) {
		this.concurrentLevel = concurrentLevel;
	}

	public void setMsgEncoding(String msgEncoding) {
		this.msgEncoding = msgEncoding;
	}
	
	/**
	 * get a flatten view of message
	 * 
	 * @param msgId	message id
	 * @return the flatten view
	 */
	public Map<String, List<String>> getFlattenViewOfMessage(Long msgId) {
		List<Map<String, String>> viewOfParsedMessage = getViewOfMessage(msgId);
		Map<String, List<String>> flattenedView = new LinkedHashMap<>();
		for (int i = 0;i<viewOfParsedMessage.size();i++) {
			Map<String, String> tagToVals = viewOfParsedMessage.get(i);
			for (Map.Entry<String, String> entry : tagToVals.entrySet()) {
				List<String> vals = flattenedView.getOrDefault(entry.getKey(), new ArrayList<String>());
				vals.add(entry.getValue());
				flattenedView.put(entry.getKey(), vals);
			}
		}
		return flattenedView;
	}

	/**
	 * get the values of a tag
	 * 
	 * @param messageId message id
	 * @param tagNumber fix tag
	 * @return the values for the fix tag
	 */
	public List<String> getTag(Long messageId, String tagNumber) {
		Map<String, List<String>> view = getFlattenViewOfMessage(messageId);
		return view.get(tagNumber);
	}
	
	private void getViewOfChildrenRepeatingGroups(Node node, List<Map<String, String>> viewOfTagGroups) {
		Map<String, String> viewOfChildrenTagGroup = new LinkedHashMap<>();
		viewOfTagGroups.add(viewOfChildrenTagGroup);
		for (Node child : node.children) {
			if (child == null) continue;
			if (viewOfChildrenTagGroup.containsKey(child.tag)) {
				Map<String, String> newView = new LinkedHashMap<>();
				newView.put(child.tag, child.val);
				viewOfTagGroups.add(newView);
			} else {
				viewOfChildrenTagGroup.put(child.tag, child.val);
			}
			if (child.children != null && child.children.length > 0) {
				getViewOfChildrenRepeatingGroups(child, viewOfTagGroups);
			} 
		}
	}
	

	/**
	 * get the map view of repeating group at the first level
	 * 
	 * @param messageId      message id
	 * @return the map view of repeating group
	 */
	public List<Map<String, String>> getViewOfMessage(Long messageId) {
		List<Map<String, String>> viewOfTagGroups = new ArrayList<>();
		
		AtomicReference<Map<String, Node>> tagRef = msgIdToMsgNode.get(messageId);
		if (tagRef == null)
			return null;

		Map<String, Node> tagToNode = tagRef.get();
		if (tagToNode == null) {
			synchronized (tagRef) {
				tagToNode = tagRef.get();
			}
			if (tagToNode == null)
				return null;
		}
		for (Map.Entry<String, Node> entry : tagToNode.entrySet()) {
			Map<String, String> viewOfTagGroup = new LinkedHashMap<>();
			String tag = entry.getKey();
			Node tagNode = entry.getValue();
			if (tagNode.children != null && tagNode.children.length > 0) {
				if (!topLevelRepeatingGroupInd.contains(tag))
					continue;
				getViewOfChildrenRepeatingGroups(tagNode, viewOfTagGroups);
			} 
			viewOfTagGroup.put(tag, tagNode.val);
			viewOfTagGroups.add(viewOfTagGroup);			
		}
		return viewOfTagGroups;
	}
	
	private void getViewOfMessage(String repeatingGroup, Map<String, List<String>> view, List<List<String>> viewOfTagGroups) {
		List<String> required = repeatingGrpIndToFirstRequired.get(repeatingGroup);
		if (required != null) {
			for (int i = 0;i<required.size();i++) {
				if (repeatingGrpIndToFirstRequired.containsKey(required.get(i)))
					getViewOfMessage(required.get(i), view, viewOfTagGroups);
				else 
					viewOfTagGroups.add(view.get(required.get(i)));
			}			
		}
		
		List<String> optional = repeatingGrpIndToFirstOptional.get(repeatingGroup);
		if (optional != null) {
			for (int i = 0;i<optional.size();i++) {
				if (repeatingGrpIndToFirstRequired.containsKey(optional.get(i)))
					getViewOfMessage(optional.get(i), view, viewOfTagGroups);
				else 
					viewOfTagGroups.add(view.get(optional.get(i)));
			}			
		}		
	}
	
	/**
	 * get the view of a specific repeating group
	 * 
	 * @param messageId	message id
	 * @param repeatingGroup repeating group tag
	 * @return the view of the specific repeating group
	 */
	public List<List<String>> getViewOfMessageByRepeatingGroup(Long messageId, String repeatingGroup) {
		List<List<String>> viewOfTagGroups = new ArrayList<>();
		Map<String, List<String>> view = getFlattenViewOfMessage(messageId);
		viewOfTagGroups.add(view.get(repeatingGroup));
		getViewOfMessage(repeatingGroup, view, viewOfTagGroups);
		return viewOfTagGroups;
	}	

	@Override
	public void afterPropertiesSet() throws Exception {
		if (repeatingGroups == null) {
			logger.info("no repeating groups are configed.");
		}
		StringTokenizer repeatingTags = new StringTokenizer(repeatingGroups, "\\|");
		while (repeatingTags.hasMoreTokens()) {
			String repeatingGroup = repeatingTags.nextToken();
			String[] tags = repeatingGroup.split(",");
			String repeatingGrpInd = tags[0];
			if (!repeatingGrpInd.startsWith("*")) {
				topLevelRepeatingGroupInd.add(repeatingGrpInd);
			} else {
				repeatingGrpInd = repeatingGrpInd.substring(1, repeatingGrpInd.length());
			}
			String firstTag = tags[1];
			List<String> requiredTags = new ArrayList<String>();
			List<String> optionalTags = new ArrayList<String>();
			requiredTags.add(firstTag);
			for (int i = 2; i < tags.length; i++) {
				String tag = tags[i];
				if (tag.endsWith("*")) {
					requiredTags.add(tag.substring(0, tag.length() - 1));
				} else  {
					optionalTags.add(tag);
				}
			}
			repeatingGrpIndToFirstRequired.put(repeatingGrpInd, requiredTags);
			repeatingGrpIndToFirstOptional.put(repeatingGrpInd, optionalTags);
		}

		es = Executors.newFixedThreadPool(concurrentLevel);
		if (msgEncoding == null) {
			msgEncoding = defaultEncoding;
		}

		errorHandlingService = Executors.newFixedThreadPool(1, new ThreadFactory() {
			public Thread newThread(Runnable r) {
				Thread t = Executors.defaultThreadFactory().newThread(r);
				t.setDaemon(true);
				return t;
			}
		});
		errorHandlingQueue = new ArrayBlockingQueue<String>(50);
		errorHandlingService.submit(new ErrorHandler(errorHandlingQueue));
	}

	@Override
	public void onMessage(Object message) throws Exception {
		if (message instanceof Message) {
			Message msg = (Message) message;

			if (msgIdToMsgNode.containsKey(msg.getMessageId())) {
				logger.error("duplicate incoming message - id::{}", msg.getMessageId());
				return;
			}
			String fixMessage = new String(msg.getMessage().array(),
					msg.getEncoding() == null ? msgEncoding : msg.getEncoding());
			es.submit(new Worker(msg.getMessageId(), fixMessage, errorHandlingQueue));
		} else {
			logger.error("incoming message type is not supported - {}", message);
		}

	}

	/*
	 * shut down FIXParser
	 * 
	 */
	public void shutdown() {
		es.shutdown();
		try {
			if (!es.awaitTermination(800, TimeUnit.MILLISECONDS)) {
				es.shutdownNow();
			}
		} catch (InterruptedException e) {
			es.shutdownNow();
		}

		errorHandlingService.shutdown();
		try {
			if (!errorHandlingService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
				errorHandlingService.shutdownNow();
			}
		} catch (InterruptedException e) {
			errorHandlingService.shutdownNow();
		}
	}

	@Override
	public void destroy() throws Exception {
		shutdown();
	}
}
