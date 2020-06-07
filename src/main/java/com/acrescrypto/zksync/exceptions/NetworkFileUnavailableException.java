package com.acrescrypto.zksync.exceptions;

public class NetworkFileUnavailableException extends ENOENTException {
	private static final long serialVersionUID = 1L;

	public NetworkFileUnavailableException(String path) {
		super(path);
	}
}
