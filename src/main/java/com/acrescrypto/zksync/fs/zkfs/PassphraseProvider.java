package com.acrescrypto.zksync.fs.zkfs;

public interface PassphraseProvider {
	public char[] passphraseForArchive(byte[] id);
}
