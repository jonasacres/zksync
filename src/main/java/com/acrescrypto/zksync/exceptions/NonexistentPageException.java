package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class NonexistentPageException extends IOException {

	public NonexistentPageException(long inodeId, int pageNum) {
		super("page not found: inode " + inodeId + ", page: " + pageNum);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 4140166865960283355L;

}
