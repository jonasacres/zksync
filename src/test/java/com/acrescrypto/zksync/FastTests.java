package com.acrescrypto.zksync;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.crypto.CryptoTests;
import com.acrescrypto.zksync.fs.FSTests;
import com.acrescrypto.zksync.net.NetTests;
import com.acrescrypto.zksync.utility.UtilityTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	FSTests.class,
	CryptoTests.class,
	NetTests.class,
	UtilityTests.class,
})

public class FastTests {
}
