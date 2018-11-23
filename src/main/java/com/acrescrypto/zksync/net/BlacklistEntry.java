package com.acrescrypto.zksync.net;

import java.nio.ByteBuffer;

import com.acrescrypto.zksync.utility.Util;

public class BlacklistEntry {
	private String address;
	private long expiration;
	
	public BlacklistEntry(String address, long durationMs) {
		this.setAddress(address);
		update(durationMs);
	}
	
	public BlacklistEntry(byte[] serialized) {
		deserialize(serialized);
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(8 + 2 + getAddress().getBytes().length);
		buf.putLong(getExpiration());
		buf.putShort((short) this.getAddress().getBytes().length);
		buf.put(getAddress().getBytes());
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.setExpiration(buf.getLong());
		byte[] addressBytes = new byte[Util.unsignShort(buf.getShort())];
		buf.get(addressBytes);
		this.setAddress(new String(addressBytes));
	}
	
	public boolean isExpired() {
		return System.currentTimeMillis() >= getExpiration();
	}
	
	public void update(long durationMs) {
		if(durationMs == Integer.MAX_VALUE) {
			this.setExpiration(Long.MAX_VALUE);
		} else {
			this.setExpiration(System.currentTimeMillis() + durationMs);
		}
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public long getExpiration() {
		return expiration;
	}

	public void setExpiration(long expiration) {
		this.expiration = expiration;
	}
}
