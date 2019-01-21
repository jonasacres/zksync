package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.BlacklistEntry;

public class XBlacklistEntry {
	private long expiration;
	private String address;
	
	public XBlacklistEntry() {}
	
	public XBlacklistEntry(String address, long expiration) {
		this.address = address;
		this.expiration = expiration;
	}
	
	public XBlacklistEntry(BlacklistEntry entry) {
		this.address = entry.getAddress();
		this.expiration = entry.getExpiration();
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
	
	public BlacklistEntry toBlacklistEntry() {
		return new BlacklistEntry(address, expiration - System.currentTimeMillis());
	}
}
