package com.acrescrypto.zksync;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.net.dht.DHTModuleTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	DHTModuleTest.class,
})

public class ModuleTests {

}
