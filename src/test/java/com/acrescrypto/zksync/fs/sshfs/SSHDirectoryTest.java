package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.fs.DirectoryTestBase;
import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class SSHDirectoryTest extends DirectoryTestBase {
	SSHFS sshscratch;

	public static char[] sshPassphrase() throws IOException {
		LocalFS fs = new LocalFS("/home/jonas");
		byte[] ppb = fs.read(".zksync-test-passphrase");
		return new String(ppb).trim().toCharArray();
	}
	
	public static SSHFS openFs() throws IOException { 
		return SSHFS.withPassphrase("zksync-test@localhost:", sshPassphrase());
	}
	
	protected static void deleteFiles() {
		try {
			openFs().rmrf("/");
		} catch(IOException exc) {}
	}
	
	@Before
	public void beforeEach() throws IOException {
		scratch = sshscratch = openFs();
		deleteFiles();
	}

	@BeforeClass
	public static void beforeClass() {
	}
}
