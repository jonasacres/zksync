package com.acrescrypto.zksync.net;

import java.nio.ByteBuffer;

import com.acrescrypto.zksync.utility.Util;

public class BlacklistEntry {
	protected String address;
	protected long expiration;
	
	public BlacklistEntry(String address, int durationMs) {
		this.address = address;
		update(durationMs);
	}
	
	public BlacklistEntry(byte[] serialized) {
		deserialize(serialized);
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(8 + 2 + address.length());
		buf.putLong(expiration);
		buf.putShort((short) this.address.length());
		buf.put(address.getBytes());
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.expiration = buf.getLong();
		byte[] addressBytes = new byte[Util.unsignShort(buf.getShort())];
		buf.get(addressBytes);
		this.address = new String(addressBytes);
	}
	
	public boolean isExpired() {
		return System.currentTimeMillis() >= expiration;
	}
	
	public void update(int newDurationMs) {
		if(newDurationMs == Integer.MAX_VALUE) {
			this.expiration = Long.MAX_VALUE;
		} else {
			this.expiration = System.currentTimeMillis() + newDurationMs;
		}
	}
}
