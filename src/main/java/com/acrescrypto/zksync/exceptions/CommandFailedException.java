package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class CommandFailedException extends IOException {

	public CommandFailedException(String command) {
		super(command);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

}
