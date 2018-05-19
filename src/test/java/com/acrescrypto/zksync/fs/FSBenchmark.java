package com.acrescrypto.zksync.fs;

import java.io.IOException;

import org.junit.Test;

import com.acrescrypto.zksync.Benchmarks;

public abstract class FSBenchmark {
	
	protected FS storage;
	
	@Test
	public void benchmarkFileCreationThroughput() throws IOException {
		Benchmarks.run("files", (i)->{
			storage.write("create-throughput-"+i, "".getBytes());
		});
		
		storage.purge();
	}
	
	@Test
	public void benchmarkFileWriteThroughput() throws IOException {
		byte[] oneKiB = new byte[1024];
		File file = storage.open("write-throughput", File.O_CREAT|File.O_TRUNC|File.O_WRONLY);
		Benchmarks.run("KiB", (i)->{
			file.write(oneKiB);
			file.flush();
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
