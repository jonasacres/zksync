package com.acrescrypto.zksync.exceptions;

public class InvalidRevisionTagException extends RuntimeException {

	public InvalidRevisionTagException(String tag) {
		super(tag);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
