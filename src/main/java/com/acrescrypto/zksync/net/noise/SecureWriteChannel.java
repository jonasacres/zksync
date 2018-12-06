package com.acrescrypto.zksync.net.noise;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

public class SecureWriteChannel implements WritableByteChannel {
	protected SecureChannel channel;
	
	public SecureWriteChannel(SecureChannel channel) {
		this.channel = channel;
	}
	
	public int write(byte[] data) throws IOException {
		return channel.write(ByteBuffer.wrap(data));
	}
	
	public int write(byte[] data, int offset, int length) throws IOException {
		return channel.write(ByteBuffer.wrap(data, offset, length));
	}
	
	@Override
	public void close() throws IOException {
		channel.close();
	}
	
	@Override
	public int write(ByteBuffer buf) throws IOException {
		return channel.write(buf);
	}

	@Override
	public boolean isOpen() {
		return channel.isOpen();
	}
}
