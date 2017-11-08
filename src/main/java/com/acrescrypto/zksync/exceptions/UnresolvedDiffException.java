package com.acrescrypto.zksync.exceptions;

import com.acrescrypto.zksync.fs.zkfs.FileDiff;

public class UnresolvedDiffException extends DiffResolutionException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6626026265071982095L;
	public FileDiff diff;
	
	public UnresolvedDiffException(FileDiff diff) {
		this.diff = diff;
	}
}
