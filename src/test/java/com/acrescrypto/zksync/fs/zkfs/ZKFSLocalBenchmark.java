package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.Benchmarks;
import com.acrescrypto.zksync.fs.FSBenchmark;

public class ZKFSLocalBenchmark extends FSBenchmark {
	static ZKMaster master;
	static ZKArchive archive;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		master = ZKMaster.openAtPath((reason)->"zksync".getBytes(), "/tmp/zkfs-benchmark");
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, ""); 
		Benchmarks.beginBenchmarkSuite("ZKFS (with "+master.storage.getClass().getSimpleName()+" storage)");
	}
	
	@Before
	public void beforeEach() throws IOException {
		storage = archive.openBlank();
	}
}
