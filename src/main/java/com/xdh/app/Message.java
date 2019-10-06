package com.xdh.app;

import java.nio.ByteBuffer;

public interface Message {
	Long getMessageId();
	String getEncoding();
	ByteBuffer getMessage();
}
