package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class InvalidPageException extends IOException {

	public InvalidPageException(String path) {
		super(path);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
