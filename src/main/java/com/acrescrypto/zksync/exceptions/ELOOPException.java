package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class ELOOPException extends IOException {
	public ELOOPException(String path) {
		super(path);
	}

	private static final long serialVersionUID = 1L;

}
