package com.acrescrypto.zksync.exceptions;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;

public class CantUnpackRevisionTagException extends RuntimeException {
	RevisionTag tag;
	
	public CantUnpackRevisionTagException(RevisionTag tag) {
		this.tag = tag;
	}
	
	public RevisionTag getTag() {
		return tag;
	}

	private static final long serialVersionUID = 1L;

}
