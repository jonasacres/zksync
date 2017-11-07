package com.acrescrypto.zksync.crypto;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	CryptoSupportTest.class,
	HashContextTest.class,
	KeyTest.class,
	PRNGTest.class
})

public class CryptoTests {

}
