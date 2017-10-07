package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class EISDIRException extends IOException {

	public EISDIRException(String path) {
		super(path + ": is directory");
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 5561756629473381223L;

}
