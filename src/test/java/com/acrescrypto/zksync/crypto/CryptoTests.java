package com.acrescrypto.zksync.crypto;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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
	static int oldLevel;
	
	@BeforeClass
	public static void beforeClass() {
		// TODO: squelch log output for ciphertext errors
	}

	@AfterClass
	public static void afterClass() {
		// TODO: reenable log output for ciphertext errors
	}
}
