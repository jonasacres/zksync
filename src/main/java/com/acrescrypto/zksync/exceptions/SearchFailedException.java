package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;

public class SearchFailedException extends IOException {
	private static final long serialVersionUID = 1L;
	protected RevisionTag revTag;
	
	public SearchFailedException(RevisionTag revTag) {
		this.revTag = revTag;
	}
	
	public RevisionTag getRevTag() {
		return revTag;
	}
	
	public String toString() {
		return "SearchFailedException: " + revTag;
	}
}
