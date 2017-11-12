package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.fs.zkfs.RefTag;

public class NonexistentPageException extends IOException {

	public NonexistentPageException(RefTag tag, int pageNum) {
		super("page not found: revtag " + Util.bytesToHex(tag.getBytes()) + ", page: " + pageNum);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 4140166865960283355L;

}
