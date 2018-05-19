package com.acrescrypto.zksync.fs.localfs;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.Benchmarks;
import com.acrescrypto.zksync.fs.FSBenchmark;

public class LocalFSBenchmark extends FSBenchmark {
	@BeforeClass
	public static void beforeAll() {
		Benchmarks.beginBenchmarkSuite("LocalFS");
	}
	
	@Before
	public void beforeEach() throws IOException {
		storage = new LocalFS("/tmp/localfs-benchmark");
		if(!storage.exists("/")) storage.mkdir("/");
	}
	
	@After
	public void afterEach() throws IOException {
		storage.purge();
	}
}
