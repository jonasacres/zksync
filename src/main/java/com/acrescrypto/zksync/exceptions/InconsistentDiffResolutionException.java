package com.acrescrypto.zksync.exceptions;

import com.acrescrypto.zksync.fs.zkfs.FileDiff;

public class InconsistentDiffResolutionException extends DiffResolutionException {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3525958611942926868L;
	FileDiff diff;
	
	public InconsistentDiffResolutionException(FileDiff diff) {
		this.diff = diff;
	}
}
