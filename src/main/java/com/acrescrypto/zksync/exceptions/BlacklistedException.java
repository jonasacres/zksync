package com.acrescrypto.zksync.exceptions;

public class BlacklistedException extends Exception {
	protected String address;

	public BlacklistedException(String address) {
		this.address = address;
	}
	
	public String toString() {
		return "Blacklisted address: " + this.address;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
