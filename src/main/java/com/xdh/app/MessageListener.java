package com.xdh.app;

/**
 * Message Listener interface
 * 
 * @author haihuang
 *
 * @param <T>
 */
public interface MessageListener<T> {
	void onMessage(T message) throws Exception;
}
