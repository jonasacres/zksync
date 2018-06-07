package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class RequestPoolTest {
	class DummyArchive extends ZKArchive {
		protected DummyArchive(ZKArchiveConfig config) throws IOException {
			super(config);
		}
	}
	
	class DummyConnection extends PeerConnection {
		boolean requestedAll, mockSeedOnly;
		int requestedPriority;
		LinkedList<RefTag> requestedRefTags = new LinkedList<>();
		LinkedList<RefTag> requestedRevisions = new LinkedList<>();
		LinkedList<Long> requestedPageTags = new LinkedList<>();
		
		@Override public void requestAll() { requestedAll = true; }
		@Override public void requestPageTags(int priority, Collection<Long> pageTags) {
			requestedPriority = priority;
			requestedPageTags.addAll(pageTags);
		}
		
		@Override public void requestRefTags(int priority, Collection<RefTag> refTags) throws PeerCapabilityException { 
			if(mockSeedOnly) throw new PeerCapabilityException();
			requestedPriority = priority;
			requestedRefTags.addAll(refTags);
		}
		
		@Override public void requestRevisionContents(int priority, Collection<RefTag> refTags) throws PeerCapabilityException {
			if(mockSeedOnly) throw new PeerCapabilityException();
			requestedPriority = priority;
			requestedRevisions.addAll(refTags);
		}
	}
	
	CryptoSupport crypto;
	ZKMaster master;
	DummyArchive archive;
	DummyConnection conn;
	RequestPool pool;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = new CryptoSupport();
		master = ZKMaster.openBlankTestVolume();
		
		ArchiveAccessor accessor = master.makeAccessorForRoot(new Key(crypto), false);
		ZKArchiveConfig config = new ZKArchiveConfig(accessor, "", ZKArchive.DEFAULT_PAGE_SIZE);
		archive = new DummyArchive(config);
		
		conn = new DummyConnection();
		pool = new RequestPool(archive);
	}
	
	@After
	public void afterEach() {
		pool.stop();
		RequestPool.pruneIntervalMs = RequestPool.DEFAULT_PRUNE_INTERVAL_MS;
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testAddPageTagShort() {
		assertFalse(pool.hasPageTag(12, 1234l));
		pool.addPageTag(12, 1234l);
		assertTrue(pool.hasPageTag(12, 1234l));
	}
	
	@Test
	public void testAddPageTagBytes() {
		byte[] pageTag = crypto.rng(crypto.hashLength());
		long shortTag = Util.shortTag(pageTag);
		assertFalse(pool.hasPageTag(19, shortTag));
		pool.addPageTag(19, pageTag);
		assertTrue(pool.hasPageTag(19, shortTag));
	}
	
	@Test
	public void testAddRefTag() throws IOException {
		RefTag refTag = archive.openBlank().commit();
		assertFalse(pool.hasRefTag(271828183, refTag));
		pool.addRefTag(271828183, refTag);
		assertTrue(pool.hasRefTag(271828183, refTag));
	}
	
	@Test
	public void testAddRevision() throws IOException {
		RefTag revTag = archive.openBlank().commit();
		assertFalse(pool.hasRevision(-123456, revTag));
		pool.addRevision(-123456, revTag);
		assertTrue(pool.hasRevision(-123456, revTag));
	}
	
	@Test
	public void testSetRequestingEverything() {
		assertFalse(pool.requestingEverything);
		pool.setRequestingEverything(true);
		assertTrue(pool.requestingEverything);
		pool.setRequestingEverything(false);
		assertFalse(pool.requestingEverything);
	}
	
	@Test
	public void testAddRequestsToPeerCallsRequestAllWhenRequestEverythingSet() {
		pool.setRequestingEverything(true);
		assertFalse(conn.requestedAll);
		pool.addRequestsToConnection(conn);
		assertTrue(conn.requestedAll);
	}
	
	@Test
	public void testAddRequestsToPeerDoesntCallRequestAllWhenRequestEverythingCleared() {
		pool.addRequestsToConnection(conn);
		assertFalse(conn.requestedAll);
	}
	
	@Test
	public void testAddRequestsToPeerCallsRequestPageTags() {
		LinkedList<Long> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			long tag = ByteBuffer.wrap(crypto.rng(8)).getLong();
			tags.add(tag);
			pool.addPageTag(123, tag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedPageTags);
		assertEquals(123, conn.requestedPriority);
	}
	
	@Test
	public void testAddRequestsToPeerCallsRequestRefTags() {
		LinkedList<RefTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
			tags.add(tag);
			pool.addRefTag(321, tag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedRefTags);
		assertEquals(321, conn.requestedPriority);
	}

	@Test
	public void testAddRequestsToPeerCallsRequestRevisionContents() {
		LinkedList<RefTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
			tags.add(tag);
			pool.addRevision(-123, tag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedRevisions);
		assertEquals(-123, conn.requestedPriority);
	}
	
	@Test
	public void testAddRequestsToPeerToleratesSeedOnly() {
		conn.mockSeedOnly = true;
		RefTag tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
		pool.addRevision(0, tag);		
		pool.addRequestsToConnection(conn);
		assertTrue(conn.requestedRevisions.isEmpty());
		// real test here is verifying that no exceptions are thrown
	}
	
	@Test
	public void testPruneRemovesAcquiredPageTags() throws IOException {
		LinkedList<Long> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			long tag = ByteBuffer.wrap(crypto.rng(8)).getLong();
			tags.add(tag);
			pool.addPageTag(i, tag);
		}
		
		ByteBuffer fullTag = ByteBuffer.allocate(crypto.hashLength());
		fullTag.putLong(tags.get(3));
		fullTag.put(crypto.rng(fullTag.remaining()));
		
		archive.getStorage().write(Page.pathForTag(fullTag.array()), new byte[0]);
		pool.prune();
		for(int i = 0; i < tags.size(); i++) {
			assertEquals(i != 3, pool.hasPageTag(i, tags.get(i)));
		}
	}
	
	@Test
	public void testPruneRemovesAcquiredRefTags() throws IOException {
		ZKFS fs = archive.openBlank();
		fs.write("testpath", new byte[2*archive.getConfig().getPageSize()]);
		RefTag realTag = fs.inodeForPath("testpath").getRefTag();

		LinkedList<RefTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag;
			do {
				tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
			} while(tag.getRefType() == RefTag.REF_TYPE_IMMEDIATE);
			tags.add(tag);
			pool.addRefTag(i, tag);
		}
		
		pool.addRefTag(-1, realTag);
		assertTrue(pool.hasRefTag(-1, realTag));
		pool.prune();
		for(int i = 0; i < tags.size(); i++) {
			assertTrue(pool.hasRefTag(i, tags.get(i)));
		}
		
		assertFalse(pool.hasRefTag(-1, realTag));
	}
	
	@Test
	public void testPruneRemovesAcquiredRevisions() throws IOException {
		RefTag realTag = archive.openBlank().commit();

		LinkedList<RefTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag;
			do {
				tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
			} while(tag.getRefType() == RefTag.REF_TYPE_IMMEDIATE);
			tags.add(tag);
			pool.addRevision(i, tag);
		}
		
		pool.addRevision(-1, realTag);
		assertTrue(pool.hasRevision(-1, realTag));
		pool.prune();
		for(int i = 0; i < tags.size(); i++) {
			assertTrue(pool.hasRevision(i, tags.get(i)));
		}
		
		assertFalse(pool.hasRevision(-1, realTag));
	}
	
	@Test
	public void testPruneThreadCallsPrune() throws IOException {
		RefTag revTag = archive.openBlank().commit();
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(archive);
		pool2.addRevision(0, revTag);
		assertFalse(pool2.requestedRevisions.isEmpty());
		assertTrue(Util.waitUntil(RequestPool.pruneIntervalMs+10, ()->pool2.hasRevision(0, revTag)));
	}
	
	@Test
	public void testStopCancelsPruneThread() throws IOException {
		RefTag revTag = archive.openBlank().commit();
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(archive);
		pool2.addRevision(0, revTag);
		pool2.stop();
		assertFalse(pool2.requestedRevisions.isEmpty());
		assertFalse(Util.waitUntil(RequestPool.pruneIntervalMs+10, ()->!pool2.hasRevision(0, revTag)));
	}
	
	@Test
	public void testSerialization() throws IOException {
		LinkedList<RefTag> tags = new LinkedList<>();
		for(int i = 0; i < 64; i++) {
			tags.add(new RefTag(archive, crypto.rng(archive.refTagSize())));
			
			if(i % 3 == 0) pool.addPageTag(i, tags.peekLast().getShortHash());
			if(i % 3 == 1) pool.addRefTag(i, tags.peekLast());
			if(i % 3 == 2) pool.addRevision(i, tags.peekLast());
		}
		
		pool.write();
		RequestPool pool2 = new RequestPool(archive);
		pool2.read();
		
		assertEquals(pool.requestingEverything, pool2.requestingEverything);
		for(int i = 0; i < 64; i++) {
			if(i % 3 == 0) assertTrue(pool2.hasPageTag(i, tags.get(i).getShortHash()));
			if(i % 3 == 1) assertTrue(pool2.hasRefTag(i, tags.get(i)));
			if(i % 3 == 2) assertTrue(pool2.hasRevision(i, tags.get(i)));
		}
		
		pool2.stop();
	}
	
	@Test
	public void testSerializationRequestingEverything() throws IOException {
		pool.setRequestingEverything(true);
		testSerialization();
	}
	
	@Test
	public void testBackgroundThreadWritesData() {
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(archive);
		pool2.addPageTag(0, 0);
		assertTrue(Util.waitUntil(100, ()->archive.getConfig().getLocalStorage().exists(pool2.path())));
		pool2.stop();
	}
	
	@Test
	public void testBackgroundThreadDoesNotWriteDataWhenStopped() {
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(archive);
		pool2.addPageTag(0, 0);
		pool2.stop();
		assertFalse(Util.waitUntil(100, ()->archive.getConfig().getLocalStorage().exists(pool2.path())));
	}
}
