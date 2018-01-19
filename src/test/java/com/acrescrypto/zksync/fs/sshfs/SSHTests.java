package com.acrescrypto.zksync.fs.sshfs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	SSHFSTest.class,
	SSHFileTest.class,
	SSHDirectoryTest.class
})

public class SSHTests {

}
