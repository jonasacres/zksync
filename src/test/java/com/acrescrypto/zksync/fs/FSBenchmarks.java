package com.acrescrypto.zksync.fs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.fs.localfs.LocalFSBenchmark;
import com.acrescrypto.zksync.fs.ramfs.RAMFSBenchmark;
import com.acrescrypto.zksync.fs.zkfs.ZKFSLocalBenchmark;
import com.acrescrypto.zksync.fs.zkfs.ZKFSRAMBenchmark;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	RAMFSBenchmark.class,
	LocalFSBenchmark.class,
	ZKFSRAMBenchmark.class,
	ZKFSLocalBenchmark.class,
})

public class FSBenchmarks {

}
