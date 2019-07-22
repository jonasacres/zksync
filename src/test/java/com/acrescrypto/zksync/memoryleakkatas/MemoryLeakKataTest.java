package com.acrescrypto.zksync.memoryleakkatas;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

/** These are exercises intended to run in a profiler to isolate memory leaks.
 * Each kata should end with no change in allocated memory.
 * */
public class MemoryLeakKataTest {
	public interface SupervisedTask {
		void task(int iteration) throws IOException;
	}
	
	public void kataCreateArchive() throws IOException {
		try(ZKMaster master = ZKMaster.openBlankTestVolume()) {
			master.createDefaultArchive().close();
		}
	}
	
	public void kataOpenBlankZKFS(ZKArchive archive) throws IOException {
		try(ZKFS blank = archive.openBlank()) {
		}
	}
	
	public void kataCommitBlankRevision(ZKFS fs) throws IOException {
		fs.commit();
	}
	
	public void kataCommitRevisionWithFileImmediate(ZKFS fs) throws IOException {
		int hashLen = fs.getArchive().getMaster().getCrypto().hashLength();
		fs.write("file", fs.getArchive().getMaster().getCrypto().defaultPrng().getBytes(hashLen-1));
		fs.commit();
	}
	
	public void kataCommitRevisionWithFileIndirect(ZKFS fs) throws IOException {
		int pageSize = fs.getArchive().getConfig().getPageSize();
		fs.write("file", fs.getArchive().getMaster().getCrypto().defaultPrng().getBytes(pageSize-1));
		fs.commit();
	}

	public void kataCommitRevisionWithFileDoubleIndirect(ZKFS fs) throws IOException {
		int pageSize = fs.getArchive().getConfig().getPageSize();
		fs.write("file", fs.getArchive().getMaster().getCrypto().defaultPrng().getBytes(pageSize+1));
		fs.commit();
	}
	
	long currentMemoryUsed() {
		return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
	}
	
	public void superviseKata(int numIterations, int checkInterval, SupervisedTask task) throws IOException {
		System.gc();
		long baseMemoryUsed = currentMemoryUsed();
		for(int i = 0; i < numIterations; i++) {
			if(i > 0 && i % checkInterval == 0) {
				long memoryUsed = currentMemoryUsed();
				Util.debugLog(String.format("Kata supervision: %d/%d iterations, memory use %d, base %d (change: %d)",
						i,
						numIterations,
						memoryUsed,
						baseMemoryUsed,
						memoryUsed - baseMemoryUsed));
			}
			
			task.task(i);
			System.gc();
		}
	}
	
	public final static String TEST_DIR = "/tmp/zksync-memorykata";
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		try(LocalFS fs = new LocalFS(TEST_DIR)) {
			fs.purge();
		}
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
	}
	
	public ZKMaster openMaster() throws IOException {
		return ZKMaster.openAtPath(ZKMaster.demoPassphraseProvider(), TEST_DIR);
	}
	
	@Test
	public void kataLoopCreateArchive() throws IOException {
		superviseKata(10000, 100, (i)->kataCreateArchive());
	}
	
	@Test
	public void kataLoopOpenBlankZKFS() throws IOException {
		try(ZKMaster master = ZKMaster.openBlankTestVolume();
		    ZKArchive archive = master.createDefaultArchive()
		) {
			superviseKata(10000, 100, (i)->{
				if(i > 0 && i % 100 == 0) {
					Util.debugLog(master.getGlobalConfig().getSubsciptionService().dumpSubscriptions());
				}
				kataOpenBlankZKFS(archive);
			});
		}
	}
	
	@Test
	public void kataLoopCommitBlankRevision() throws IOException {
		try(ZKMaster master = openMaster();
		    ZKArchive archive = master.createDefaultArchive();
			ZKFS fs = archive.openBlank()
		) {
			fs.skipIntegrity = true;
			superviseKata(10000, 100, (i)->{
				if(i > 0 && i % 100 == 0) {
					Util.debugLog(master.getGlobalConfig().getSubsciptionService().dumpSubscriptions());
				}
				kataCommitBlankRevision(fs);
			});
		}
	}
	
	@Test
	public void kataLoopCommitImmediateFile() throws IOException {
		try(ZKMaster master = openMaster();
		    ZKArchive archive = master.createDefaultArchive();
			ZKFS fs = archive.openBlank()
		) {
			fs.skipIntegrity = true;
			superviseKata(10000, 100, (i)->{
				if(i > 0 && i % 100 == 0) {
					Util.debugLog(master.getGlobalConfig().getSubsciptionService().dumpSubscriptions());
				}
				kataCommitRevisionWithFileImmediate(fs);
			});
		}
	}
	
	@Test
	public void kataLoopCommitIndirectFile() throws IOException {
		try(ZKMaster master = openMaster();
		    ZKArchive archive = master.createDefaultArchive();
			ZKFS fs = archive.openBlank()
		) {
			fs.skipIntegrity = true;
			superviseKata(10000, 100, (i)->{
				if(i > 0 && i % 100 == 0) {
					Util.debugLog(master.getGlobalConfig().getSubsciptionService().dumpSubscriptions());
				}
				kataCommitRevisionWithFileIndirect(fs);
			});
		}
	}
	
	@Test
	public void kataLoopCommitDoubleIndirectFile() throws IOException {
		try(ZKMaster master = openMaster();
		    ZKArchive archive = master.createDefaultArchive();
			ZKFS fs = archive.openBlank()
		) {
			fs.skipIntegrity = true;
			superviseKata(10000, 100, (i)->{
				if(i > 0 && i % 100 == 0) {
					Util.debugLog(master.getGlobalConfig().getSubsciptionService().dumpSubscriptions());
				}
				kataCommitRevisionWithFileDoubleIndirect(fs);
			});
		}
	}
}
