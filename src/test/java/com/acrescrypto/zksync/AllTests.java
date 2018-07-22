package com.acrescrypto.zksync;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	FastTests.class,
	ModuleTests.class,
	IntegrationTest.class
})

public class AllTests {

}
