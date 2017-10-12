package com.acrescrypto.zksync.exceptions;

import java.io.IOException;

public class EACCESException extends IOException {

	public EACCESException(String path) {
		super("Access error: " + path);
	}
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 878369340597612236L;

}
