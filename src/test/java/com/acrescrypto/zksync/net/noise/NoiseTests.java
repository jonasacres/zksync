package com.acrescrypto.zksync.net.noise;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	CipherStateTest.class,
	SymmetricStateTest.class,
	HandshakeStateTest.class
})

public class NoiseTests {
}
