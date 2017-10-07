package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class EEXISTSException extends IOException {

	public EEXISTSException(String path) {
		super("Path exists: " + path);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 2775322692425591861L;

}
