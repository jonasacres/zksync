package com.acrescrypto.zksync;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.fs.sshfs.SSHTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	FastTests.class,
	SSHTests.class,
})

public class AllTests {

}
