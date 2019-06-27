package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class InaccessibleStorageException extends IOException {
	Throwable cause;
	/**
	 * 
	 */
	private static final long serialVersionUID = 2227953727973380010L;
	public InaccessibleStorageException() {}
	public InaccessibleStorageException(Throwable cause) { this.cause = cause; }
}
