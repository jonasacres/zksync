package com.acrescrypto.zksync.fs.zkfs;

public interface PassphraseProvider {
	public byte[] requestPassphrase(String purpose);
}
