package com.acrescrypto.zksync.fs.ramfs;

import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.Benchmarks;
import com.acrescrypto.zksync.fs.FSBenchmark;

public class RAMFSBenchmark extends FSBenchmark {
	@BeforeClass
	public static void beforeAll() {
		Benchmarks.beginBenchmarkSuite("RAMFS");
	}
	
	@Before
	public void beforeEach() {
		storage = new RAMFS();
	}
}
