package com.acrescrypto.zksync;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.crypto.CryptoTests;
import com.acrescrypto.zksync.fs.localfs.LocalTests;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	CryptoTests.class,
	LocalTests.class,
	ZKFSTests.class
})

public class ZKSyncTests {

}
