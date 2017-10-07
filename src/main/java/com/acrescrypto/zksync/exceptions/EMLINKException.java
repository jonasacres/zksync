package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class EMLINKException extends IOException {
	
	public EMLINKException(String path) {
		super(path + ": too many links");
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 7540609738915400041L;

}
