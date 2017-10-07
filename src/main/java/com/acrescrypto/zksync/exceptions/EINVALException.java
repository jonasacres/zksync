package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class EINVALException extends IOException {
	public EINVALException(String error) {
		super("Invalid operation: " + error);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1849395774186970589L;

}
