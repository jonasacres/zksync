package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class EISNOTDIRException extends IOException {

	public EISNOTDIRException(String path) {
		super(path + ": is not a directory");
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = -1103170400987572535L;

}
