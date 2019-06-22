package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class SwarmTimeoutException extends IOException {
	public SwarmTimeoutException(String reason) {
		super(reason);
	}

	private static final long serialVersionUID = 1L;
}
