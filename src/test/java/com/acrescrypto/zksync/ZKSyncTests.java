package com.acrescrypto.zksync;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.crypto.CryptoTests;
import com.acrescrypto.zksync.fs.localfs.LocalTests;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	CryptoTests.class,
	LocalTests.class,
	ZKFSTest.class
})

public class ZKSyncTests {

}
