package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;

public class SearchFailedException extends IOException {
	RevisionTag tag;
	
	public SearchFailedException(RevisionTag tag) {
		this.tag = tag;
	}
	
	public RevisionTag getTag() {
		return tag;
	}

	private static final long serialVersionUID = 1L;
}
