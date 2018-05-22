package com.acrescrypto.zksync.fs;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.acrescrypto.zksync.Benchmarks;
import com.acrescrypto.zksync.exceptions.BenchmarkFinishedException;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public abstract class FSBenchmark {
	
	protected FS storage;
	
	@AfterClass
	public static void finishSuite() {
		Benchmarks.finishBenchmarkSuite();
	}
	
	@Test
	public void benchmarkFileCreationThroughput() throws IOException {
		Benchmarks.run("files", (i)->{
			storage.write("create-throughput-"+i, "".getBytes());
		});
		
		storage.purge();
	}
	
	@Test
	public void benchmarkFileWriteThroughput() throws IOException {
		byte[] oneMiB = new byte[1024*1024];
		File file = storage.open("write-throughput", File.O_CREAT|File.O_TRUNC|File.O_WRONLY);
		Benchmarks.run("MiB", (i)->{
			if(oneMiB.length * i > storage.maxFileSize()) throw new BenchmarkFinishedException();
			file.write(oneMiB);
		});
		
		file.close();
		storage.purge();
	}
	
	@Test
	public void benchmarkFileReadThroughput() throws IOException {
		// this test has a definite risk of getting misled by caching
		byte[] oneMiB = new byte[1024*1024];
		storage.write("read-throughput", oneMiB);
		Benchmarks.run("MiB", (i)->{
			storage.read("read-throughput");
		});
		storage.purge();
	}
}
