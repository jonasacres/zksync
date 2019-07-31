package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class BlockDoesNotContainPageException extends IOException {
	private static final long serialVersionUID = 1L;
	long identity, pageNum;
	
	public BlockDoesNotContainPageException(long identity, long pageNum) {
		this.identity = identity;
		this.pageNum = pageNum;
	}
	
	public long getIdentity() {
		return identity;
	}
	
	public long getPageNum() {
		return pageNum;
	}
}
