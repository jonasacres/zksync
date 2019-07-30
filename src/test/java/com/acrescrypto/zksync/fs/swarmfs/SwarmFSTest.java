package com.acrescrypto.zksync.fs.swarmfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.PeerSwarm;

public class SwarmFSTest {
	class DummyPeerSwarm extends PeerSwarm {
		boolean blocking;
		StorageTag requestedTag;
		int requestedPriority;
		
		public DummyPeerSwarm(ZKArchiveConfig config) throws IOException {
			super(config);
		}
		
		@Override
		public void requestTag(int priority, StorageTag tag) {
			requestedPriority = priority;
			requestedTag = tag;
		}
		
		@Override
		public synchronized void waitForPage(int priority, StorageTag tag, long timeoutMs) {
			requestTag(priority, tag);
			if(!blocking) return;
			try { this.wait(); } catch(InterruptedException exc) {}
		}
		
		public synchronized void received() {
			this.notifyAll();
		}
	}
	
	ZKMaster master;
	ZKArchive archive;
	SwarmFS swarmFs;
	DummyPeerSwarm swarm;
	StorageTag tag;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		ZKFS fs = archive.openBlank();
		fs.write("test", new byte[archive.getConfig().getPageSize()]);
		tag = new PageTree(fs.inodeForPath("test")).getPageTag(0);
		
		swarm = new DummyPeerSwarm(archive.getConfig());
		swarmFs = new SwarmFS(swarm);
		fs.close();
	}
	
	@After
	public void afterEach() throws IOException {
		swarmFs.close();
		swarm.close();
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Test
	public void getSwarm() {
		assertEquals(swarm, swarmFs.getSwarm());
	}
	
	@Test
	public void testReadRequestsTagFromSwarm() throws IOException {
		swarmFs.read(tag.path());
		assertTrue(tag.equals(swarm.requestedTag));
		assertEquals(SwarmFS.REQUEST_PRIORITY, swarm.requestedPriority);
	}
	
	@Test
	public void testReadBlocksUntilPageReceivedFromSwarm() throws InterruptedException {
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()-> {
			try {
				swarmFs.read(tag.path());
				holder.waited = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		
		swarm.blocking = true;
		thread.start();
		Thread.sleep(10);
		assertFalse(holder.waited);
		
		swarm.received();
		thread.join(100);
		
		// TODO Urgent: (itf) linux 12/17/2018 UniversalTests dd5bb5b6+ linux AssertionFailed
		assertTrue(holder.waited);
	}
	
	@Test
	public void testReadReturnsCopyFromCachedStorage() throws IOException {
		byte[] expectedData = archive.getConfig().getCacheStorage().read(tag.path());
		byte[] data = swarmFs.read(tag.path());
		
		assertTrue(Arrays.equals(expectedData, data));
	}
	
	@Test
	public void testOpenRequestsTagFromSwarm() throws IOException {
		swarmFs.open(tag.path(), File.O_RDONLY).close();
		assertTrue(tag.equals(swarm.requestedTag));
		assertEquals(SwarmFS.REQUEST_PRIORITY, swarm.requestedPriority);
	}

	@Test
	public void testOpenBlocksUntilPageReceivedFromSwarm() throws InterruptedException {
		class Holder { boolean waited; }
		Holder holder = new Holder();
		
		Thread thread = new Thread(()-> {
			try {
				swarmFs.open(tag.path(), File.O_RDONLY).close();
				holder.waited = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		
		swarm.blocking = true;
		thread.start();
		Thread.sleep(10);
		assertFalse(holder.waited);
		
		swarm.received();
		thread.join(100);
		assertTrue(holder.waited);
	}
	
	@Test
	public void testOpenReturnsFileHandleFromCachedStorage() throws IOException {
		try(
			File expected = archive.getConfig().getCacheStorage().open(tag.path(), File.O_RDONLY);
			File actual = swarmFs.open(tag.path(), File.O_RDONLY)
		) {
				assertEquals(expected.getClass(), actual.getClass());
		}
	}
	
	@Test
	public void testStatDoesNotRequestPage() throws IOException {
		swarmFs.stat(tag.path());
		assertNull(swarm.requestedTag);
	}
	
	@Test(expected=ENOENTException.class)
	public void testStatThrowsENOENTForNonTags() throws IOException {
		swarmFs.stat("test");
	}
	
	@Test
	public void testStatSetsSizeToPageSize() throws IOException {
		assertEquals(swarm.getConfig().getSerializedPageSize(), swarmFs.stat(tag.path()).getSize());
	}
	
	@Test
	public void testStatSetsTypeToRegularFile() throws IOException {
		assertEquals(Stat.TYPE_REGULAR_FILE, swarmFs.stat(tag.path()).getType());
	}
}
