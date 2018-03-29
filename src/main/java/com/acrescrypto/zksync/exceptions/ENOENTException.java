package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class ENOENTException extends IOException {
	
	public ENOENTException(String path) {
		super(path + ": no such file or directory");
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -8812124167099433790L;
}
