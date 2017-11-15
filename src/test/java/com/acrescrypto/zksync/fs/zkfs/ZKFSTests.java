package com.acrescrypto.zksync.fs.zkfs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	RevisionTreeTest.class,
	ZKFSTest.class,
	ZKDirectoryTest.class,
	ZKFileTest.class,
	DiffSetTest.class
})

public class ZKFSTests {

}
