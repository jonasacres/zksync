package com.acrescrypto.zksync.fs;

import java.io.IOException;

public interface TimedReader {
	public byte[] read(String path, long timeoutRemainingMs) throws IOException;
}
