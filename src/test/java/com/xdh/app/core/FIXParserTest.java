package com.xdh.app.core;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.xdh.app.Message;
import com.xdh.app.core.FIXParser;

import junit.framework.Assert;

/**
 * Unit test for FIXParser App.
 * 
 * @param <T>
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("/Spring-FIX.xml")
public class FIXParserTest<T> {
	@Autowired
	FIXParser<T> fixParser;
	ArrayList<Message> messages;
	AtomicLong id = new AtomicLong(1);
	int numOfMessages = 10_000;

	@Before
	public void setup() {
		final String strMsg1 = "8=345|9=12|55=IBM|40=P|269=2|277=12|283=5|456=7|277=1|231=56|456=7|44=12|";
		messages = new ArrayList<>();
		messages.ensureCapacity(numOfMessages);

		Message single = new Message() {

			@Override
			public Long getMessageId() {
				return Long.valueOf(1);
			}

			@Override
			public String getEncoding() {
				return "UTF-8";
			}

			@Override
			public ByteBuffer getMessage() {
				return ByteBuffer.wrap(strMsg1.getBytes());
			}

		};
		messages.add(single);
		
		final String strMsg2 = "8=345|9=12|55=IBM|40=P|269=2|277=12|283=5|456=7|277=1|231=56|456=7|123=2|786=9|398=ABC|786=QAS|567=12|496=SDF|398=12|44=12|";

		messages.add(new Message() {

			@Override
			public Long getMessageId() {
				return Long.valueOf(2);
			}

			@Override
			public String getEncoding() {
				return "UTF-8";
			}

			@Override
			public ByteBuffer getMessage() {
				return ByteBuffer.wrap(strMsg2.getBytes());
			}
		});	
		
		final String strMsg3 = "8=345|9=12|55=IBM|40=P|269=2|277=12|283=5|456=7|277=1|231=56|44=12|";

		messages.add(new Message() {

			@Override
			public Long getMessageId() {
				return Long.valueOf(3);
			}

			@Override
			public String getEncoding() {
				return "UTF-8";
			}

			@Override
			public ByteBuffer getMessage() {
				return ByteBuffer.wrap(strMsg3.getBytes());
			}

		});		
		final String strMsg4 = "8=345|9=12|55=IBM|73=2|11=PRAGMA01|63=Dx|78=2|79=1|467=20|79=2|467=21|64=20190318|11=PRAGMA02|63=Yx|78=2|79=3|467=22|79=4|467=23|64=20190319|40=P|269=2|277=12|283=5|456=7|277=1|231=56|456=7|123=2|786=9|398=ABC|786=QAS|567=12|496=SDF|398=12|44=12|";

		messages.add(new Message() {

			@Override
			public Long getMessageId() {
				return Long.valueOf(4);
			}

			@Override
			public String getEncoding() {
				return "UTF-8";
			}

			@Override
			public ByteBuffer getMessage() {
				return ByteBuffer.wrap(strMsg4.getBytes());
			}
		});			
	}

	@After
	public void tearDown() {

	}
	
	/**
	 * verify a message
	 * 
	 * @param msgId                  message id
	 * @param message                fix message
	 */
	private void verifyTagValue(Long msgId, String message) {
		StringTokenizer tokenizer = new StringTokenizer(message, "\\|");
		Map<String, List<String>> tagToValue = new LinkedHashMap<>();
		while (tokenizer.hasMoreTokens()) {
			String[] pair = tokenizer.nextToken().split("=");
			List<String> children = tagToValue.getOrDefault(pair[0], new ArrayList<String>());
			children.add(pair[1]);
			tagToValue.put(pair[0], children);
		}
		
		List<Map<String, String>> viewOfParsedMessage = fixParser.getViewOfMessage(msgId);
		Map<String, List<String>> flattenedView = new LinkedHashMap<>();
		for (int i = 0;i<viewOfParsedMessage.size();i++) {
			Map<String, String> tagToVals = viewOfParsedMessage.get(i);
			for (Map.Entry<String, String> entry : tagToVals.entrySet()) {
				List<String> vals = flattenedView.getOrDefault(entry.getKey(), new ArrayList<String>());
				vals.add(entry.getValue());
				flattenedView.put(entry.getKey(), vals);
			}
		}
		
		Assert.assertEquals(tagToValue.size(), flattenedView.size());
		
		for (Iterator<Map.Entry<String, List<String>>> it = tagToValue.entrySet().iterator(); it.hasNext();) {
			Map.Entry<String, List<String>> entry = it.next();
			Assert.assertEquals(entry.getValue(), flattenedView.get(entry.getKey()));
		}	
	}
	
	@Test
	public void testGetTagWithSingleRepeatingGroup() throws Exception {
		Message msg = messages.get(0);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				new ArrayBlockingQueue<String>(1));
		worker.run();
		verifyTagValue(msg.getMessageId(), message);
	}
	
	@Test
	public void testGetTagWithMultiRepeatingGroups() throws Exception {
		Message msg = messages.get(1);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				new ArrayBlockingQueue<String>(1));
		worker.run();
		verifyTagValue(msg.getMessageId(), message);		
	}
	
	@Test
	public void testMalformatedMessage() throws Exception {
		Message msg = messages.get(2);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		Map<String, List<String>> repeatingGroupChildren = new LinkedHashMap<>();
		String[] rpgChildren = new String[] { "277", "231", "283", "456" };
		List<String> repeatingGroupChildlst = Arrays.asList(rpgChildren);
		repeatingGroupChildren.put("269", repeatingGroupChildlst);
		BlockingQueue<String> errorMsgs = new ArrayBlockingQueue<>(1);

		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				errorMsgs);
		worker.run();
		Assert.assertEquals(1, errorMsgs.size());
		String errMsg = errorMsgs.take();
		Assert.assertEquals(message, errMsg );
	}
	
	@Test
	public void testGetTagWithMultiAndNestedRepeatingGroups() throws Exception {
		Message msg = messages.get(3);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				new ArrayBlockingQueue<String>(1));
		worker.run();
		verifyTagValue(msg.getMessageId(), message);
	}
	
	@Test
	public void testGetTag() throws Exception {
		Message msg = messages.get(3);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				new ArrayBlockingQueue<String>(1));
		worker.run();
		Assert.assertEquals("[IBM]", fixParser.getTag(msg.getMessageId(), "55").toString());
		Assert.assertEquals("[PRAGMA01, PRAGMA02]", fixParser.getTag(msg.getMessageId(), "11").toString());
		Assert.assertEquals("[1, 2, 3, 4]", fixParser.getTag(msg.getMessageId(), "79").toString());
	}
	
	@Test
	public void testGetRepeatingGroup() throws Exception {
		Message msg = messages.get(1);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				new ArrayBlockingQueue<String>(1));
		worker.run();
		System.out.println(fixParser.getViewOfMessageByRepeatingGroup(msg.getMessageId(), "269"));
		System.out.println(fixParser.getViewOfMessageByRepeatingGroup(msg.getMessageId(), "123"));
		String expected269 = "[[2], [12, 1], [7, 7], [56], [5]]";
		Assert.assertEquals(expected269, fixParser.getViewOfMessageByRepeatingGroup(msg.getMessageId(), "269").toString());
		String expected123 = "[[2], [9, QAS], [ABC, 12], [12], [SDF]]";
		Assert.assertEquals(expected123,fixParser.getViewOfMessageByRepeatingGroup(msg.getMessageId(), "123").toString());
	}	
	
	@Test
	public void testGetNestedRepeatingGroup() throws Exception {
		Message msg = messages.get(3);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				new ArrayBlockingQueue<String>(1));
		worker.run();
		System.out.println(fixParser.getViewOfMessageByRepeatingGroup(msg.getMessageId(), "78"));
		String expected73 = "[[2], [PRAGMA01, PRAGMA02], [20190318, 20190319], null, [1, 2, 3, 4], [20, 21, 22, 23], [Dx, Yx]]";
		Assert.assertEquals(expected73, fixParser.getViewOfMessageByRepeatingGroup(msg.getMessageId(), "73").toString());
		String expected78 = "[[2, 2], [1, 2, 3, 4], [20, 21, 22, 23]]";
		Assert.assertEquals(expected78,fixParser.getViewOfMessageByRepeatingGroup(msg.getMessageId(), "78").toString());
	}
	
	@Test
	public void testFlattenView() throws Exception {
		Message msg = messages.get(3);
		String message = new String(msg.getMessage().array(), msg.getEncoding() == null ? "UTF-8" : msg.getEncoding());
		FIXParser<T>.Worker worker = fixParser.new Worker(msg.getMessageId(), message,
				new ArrayBlockingQueue<String>(1));
		worker.run();
		String expected = "[{8=345}, {9=12}, {55=IBM}, {11=PRAGMA01}, {63=Dx, 78=2, 64=20190318}, {79=1}, {467=20}, {79=2}, {467=21}, {11=PRAGMA02}, {63=Yx, 78=2, 64=20190319}, {79=3}, {467=22}, {79=4}, {467=23}, {73=2}, {40=P}, {277=12}, {283=5, 456=7}, {277=1}, {231=56, 456=7}, {269=2}, {786=9}, {398=ABC}, {786=QAS}, {567=12, 496=SDF, 398=12}, {123=2}, {44=12}]"; 
				
		Assert.assertEquals(expected, fixParser.getViewOfMessage(msg.getMessageId()).toString());
	}
	
}
