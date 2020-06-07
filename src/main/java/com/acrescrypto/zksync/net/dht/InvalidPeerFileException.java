package com.acrescrypto.zksync.net.dht;

import java.io.IOException;

public class InvalidPeerFileException extends IOException {
	public InvalidPeerFileException(String string) {
		super(string);
	}

	private static final long serialVersionUID = 1L;
}
