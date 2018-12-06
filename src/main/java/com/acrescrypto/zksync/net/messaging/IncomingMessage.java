package com.acrescrypto.zksync.net.messaging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.SelectorProvider;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.utility.AppendableInputStream;
import com.acrescrypto.zksync.utility.Util;
import com.fasterxml.jackson.databind.JsonNode;

public class IncomingMessage extends AbstractSelectableChannel implements ReadableByteChannel {
	protected long messageId;
	protected String command, app;
	protected JsonNode parsed;
	protected boolean closed, finished;
	protected AppendableInputStream readBuffer;
	protected MessagingConnection connection;
	
	protected IncomingMessage(MessagingConnection connection, long messageId) {
		super(SelectorProvider.provider());
		this.connection = connection;
		this.messageId = messageId;
		this.readBuffer = new AppendableInputStream();
	}
	
	public void notifyReceivedBytes(boolean isFinal, ByteBuffer data) throws ProtocolViolationException {
		if(closed) return;
		
		if(parsed == null) {
			parseJson(data);
		}
		
		if(data.hasRemaining()) {
			readBuffer.write(data);
		}
		
		closed = finished = isFinal;
	}
	
	protected void parseJson(ByteBuffer data) throws ProtocolViolationException {
		assertState(data.remaining() > 2);
		int jsonLen = Util.unsignShort(data.getShort());
		assertState(data.remaining() >= jsonLen);
		
		ByteArrayInputStream jsonStream = new ByteArrayInputStream(data.array(), data.position(), jsonLen);
		try {
			parsed = Util.objectMapper().readTree(jsonStream);
		} catch (IOException exc) {
			throw new ProtocolViolationException();
		}
		
		assertState(parsed.has("app") && parsed.get("app").isTextual());
		assertState(parsed.has("command") && parsed.get("command").isTextual());
		
		app = parsed.get("app").textValue();
		command = parsed.get("command").textValue();
	}
	
	@Override
	public int read(ByteBuffer dst) throws IOException {
		if(readBuffer.available() < 0) return -1;
		
		int readLen;
		if(isBlocking()) {
			readLen = dst.remaining();
		} else { 
			readLen = Math.min(dst.remaining(), readBuffer.available());
		}
		
		if(readLen == 0) return 0;

		boolean completed = false;
		try {
			begin();
			
			int r = readBuffer.read(dst.array(), dst.position(), readLen);
			dst.position(dst.position() + r);
			completed = true;
			return r;
		} finally {
			end(completed);
		}
	}
	
	public int readBlocking(ByteBuffer dst) {
		return 0;
	}

	@Override
	protected void implCloseSelectableChannel() throws IOException {
		if(!finished) {
			readBuffer.close();
			connection.finishedMessage(this);
			this.finished = true;
		}
		
		this.closed = true;
	}

	@Override
	protected void implConfigureBlocking(boolean block) throws IOException {
	}

	@Override
	public int validOps() {
		return SelectionKey.OP_READ;
	}
	
	
	protected void assertState(boolean condition) throws ProtocolViolationException {
		if(!condition) throw new ProtocolViolationException();
	}
}
