package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class FileTypeNotSupportedException extends IOException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3333684165244625461L;

	public FileTypeNotSupportedException(String string) {
		super(string);
	}
}
