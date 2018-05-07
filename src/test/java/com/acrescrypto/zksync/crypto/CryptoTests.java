package com.acrescrypto.zksync.crypto;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.utility.Logger;

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
		oldLevel = Logger.setLevel(Logger.LOG_FATAL);
	}

	@AfterClass
	public static void afterClass() {
		Logger.setLevel(oldLevel);
	}
}
