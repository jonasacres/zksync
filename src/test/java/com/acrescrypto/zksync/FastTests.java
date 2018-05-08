package com.acrescrypto.zksync;

import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoTests;
import com.acrescrypto.zksync.crypto.ch;
import com.acrescrypto.zksync.fs.localfs.LocalTests;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	CryptoTests.class,
	LocalTests.class,
	ZKFSTests.class
})

public class FastTests {
}
