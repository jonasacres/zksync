package com.acrescrypto.zksync.fs.ramfs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	RAMFSTest.class,
	RAMDirectoryTest.class,
	RAMFileTest.class
})

public class RAMFSTests {

}
