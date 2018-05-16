package com.acrescrypto.zksync.net;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;

public class PageQueueTest {
	static ZKArchive archive;
	static ZKMaster master;
	static RefTag refTag, revTag;
	static byte[] pageTag;
	
	PageQueue queue;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openAtPath((String reason) -> { return "zksync".getBytes(); }, "/tmp/zksync-chunkaccumulator");
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		// TODO: write some data...
	}
	
	@Before
	public void beforeEach() {
		queue = new PageQueue(archive);
	}
	
	@After
	public void afterEach() {
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test @Ignore
	public void testHashChunkReturnsFalseIfNoChunkAvailable() {
	}
	
	// hasNextChunk returns true if there is a next chunk available in the queue
	// hasNextChunk returns false if there is not a next chunk available in the queue
	
	// nextChunk returns next chunk from queue; blocks until available
	// nextChunk shuffles items within a given priority level
	// nextChunk sends higher priority items before lower priority items
	
	// startSendingEverything adds an everything item
	// stopSendingEverything removes the everything item
	// everything sends all pages
	// everything sends pages in random order
	
	// addPageTag enqueues all chunks in a page
	// addPageTag honors priority setting
	// addPageTag sends chunks in shuffled order
	// addPageTag tolerates non-existent pages
	
	// addRefTagContents enqueues all pages in a reftag
	// addRefTagContents honors priority setting
	// addRefTagContents sends pages in shuffled order
	// addRefTagContents tolerates non-existent reftags
	// addRefTagContents tolerates immediate reftags
	// addRefTagContents tolerates 1-page reftags
	// addRefTagContents tolerates multipage reftags
	
	// addRevisionTag enqueues all reftags in a revision
	// addRevisionTag honors priority setting
	// addRevisionTag includes inode table
	// addRevisionTag sends children in shuffled order
	// addRevisionTag tolerates non-existent revision tags
	
	// stop all empties queue
}
