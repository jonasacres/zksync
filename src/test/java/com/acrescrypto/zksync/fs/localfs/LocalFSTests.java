package com.acrescrypto.zksync.fs.localfs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	LocalFSTest.class,
	LocalFileTest.class,
	LocalDirectoryTest.class
})

public class LocalFSTests {

}
