package com.acrescrypto.zksyncweb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.AllTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	AllTests.class,
	WebTests.class,
})

public class UniversalTests {

}
