package com.acrescrypto.zksync.net.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.HashMap;

import com.acrescrypto.zksync.net.noise.SecureChannel;
import com.acrescrypto.zksync.utility.Util;
import com.fasterxml.jackson.core.JsonProcessingException;

public class OutgoingMessage extends AbstractSelectableChannel implements ByteChannel {
	protected long messageId;
	protected String command, app;
	protected Object data;
	protected boolean finished;
	protected MessagingConnection connection;
	protected ReadableByteChannel payloadChannel; 
	
	public static OutgoingMessage simpleMessage(MessagingConnection conn, String app, String cmd) throws IOException {
		return simpleMessage(conn, app, cmd,
				null);
	}

	public static OutgoingMessage simpleMessage(MessagingConnection conn, String app, String cmd, Object data) throws IOException {
		return new OutgoingMessage(conn, app, cmd, data)
				.sendWithoutPayload();
	}
	
	public static OutgoingMessage messageWithPayload(MessagingConnection conn, String app, String cmd, Object data, ByteBuffer payload) throws IOException {
		return new OutgoingMessage(conn, app, cmd, data)
				.addPayload(payload)
				.start();
	}
	
	public OutgoingMessage(MessagingConnection conn, String app, String cmd, Object data) {
		super(SelectorProvider.provider());
	}
	
	public OutgoingMessage start() throws IOException {
		// if the payload is complete, mark us as finished
		// send the json, and any bytes of the payload that are immediately available
		return this;
	}
	
	public OutgoingMessage finish() throws IOException {
		// send just our finishing message ID plus any bytes left in the payload
		return this;
	}
	
	public OutgoingMessage sendWithoutPayload() throws IOException {
		ByteBuffer frame = ByteBuffer.allocate(SecureChannel.MAX_MESSAGE_LEN);
		frame.putLong(messageId | Long.MIN_VALUE); // set MSB to indicate final
		frame.put(makeJson().getBytes());
		connection.channel.write(ByteBuffer.wrap(frame.array(), 0, frame.position()));
		close();
		return this;
	}
	
	public String makeJson() throws JsonProcessingException {
		HashMap<String,Object> map = new HashMap<>();
		map.put("command", command);
		map.put("app", app);
		map.put("data", data);
		return Util.objectMapper().writeValueAsString(map);
	}
	
	public OutgoingMessage addPayload(ByteBuffer buf) {
		return this;
	}
	
	public OutgoingMessage addPayload(ReadableByteChannel channel) {
		return this;
	}
	
	@Override
	public int write(ByteBuffer src) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	public int read(ByteBuffer dst) throws IOException {
		// TODO Auto-generated method stub
		return 0;
	}
	
	@Override
	protected void implCloseSelectableChannel() throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	protected void implConfigureBlocking(boolean block) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public int validOps() {
		// TODO Auto-generated method stub
		return 0;
	}
}
