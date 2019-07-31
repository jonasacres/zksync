package com.acrescrypto.zksync.exceptions;

import com.acrescrypto.zksync.fs.zkfs.StorageTag;

public class StorageTagRewriteException extends RuntimeException {
	private static final long serialVersionUID = 1L;
	StorageTag storageTag;
	
	public StorageTagRewriteException(StorageTag storageTag) {
		this.storageTag = storageTag;
	}
	
	public String toString() {
		return "StorageTagRewriteException: " + storageTag;
	}
}
