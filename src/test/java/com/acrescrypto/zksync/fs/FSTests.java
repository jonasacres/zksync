package com.acrescrypto.zksync.fs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.fs.backedfs.BackedFSTest;
import com.acrescrypto.zksync.fs.localfs.LocalFSTests;
import com.acrescrypto.zksync.fs.ramfs.RAMFSTests;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	FSPathTest.class,
	LocalFSTests.class,
	RAMFSTests.class,
	ZKFSTests.class,
	BackedFSTest.class,
	SwarmFSTest.class
})

public class FSTests {

}
