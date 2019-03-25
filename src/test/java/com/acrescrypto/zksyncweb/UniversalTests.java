package com.acrescrypto.zksyncweb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.EngineTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	EngineTests.class,
	WebTests.class,
})

public class UniversalTests {

}
