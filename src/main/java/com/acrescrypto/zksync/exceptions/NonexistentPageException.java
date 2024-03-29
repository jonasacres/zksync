package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.utility.Util;

public class NonexistentPageException extends IOException {

	public NonexistentPageException(RefTag tag, int pageNum) {
		super("page not found: revtag " + Util.formatRefTag(tag) + ", page: " + pageNum);
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 4140166865960283355L;

}
