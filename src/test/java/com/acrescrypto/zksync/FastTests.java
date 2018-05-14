package com.acrescrypto.zksync;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.crypto.CryptoTests;
import com.acrescrypto.zksync.fs.backedfs.BackedFSTest;
import com.acrescrypto.zksync.fs.localfs.LocalTests;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTests;
import com.acrescrypto.zksync.net.NetTests;
import com.acrescrypto.zksync.utility.UtilityTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	CryptoTests.class,
	LocalTests.class,
	ZKFSTests.class,
	NetTests.class,
	UtilityTests.class,
	BackedFSTest.class
})

public class FastTests {
}
