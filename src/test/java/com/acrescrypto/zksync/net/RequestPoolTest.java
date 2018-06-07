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
		LinkedList<RefTag> requestedRefTags = new LinkedList<>();
		LinkedList<RefTag> requestedRevisions = new LinkedList<>();
		LinkedList<Long> requestedPageTags = new LinkedList<>();
		
		@Override public void requestAll() { requestedAll = true; }
		@Override public void requestPageTags(Collection<Long> pageTags) { requestedPageTags.addAll(pageTags); }
		@Override public void requestRefTags(Collection<RefTag> refTags) throws PeerCapabilityException { 
			if(mockSeedOnly) throw new PeerCapabilityException();
			requestedRefTags.addAll(refTags);
		}
		
		@Override public void requestRevisionContents(Collection<RefTag> refTags) throws PeerCapabilityException {
			if(mockSeedOnly) throw new PeerCapabilityException();
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
		assertFalse(pool.requestedPageTags.contains(1234l));
		pool.addPageTag(1234l);
		assertTrue(pool.requestedPageTags.contains(1234l));
	}
	
	@Test
	public void testAddPageTagBytes() {
		byte[] pageTag = crypto.rng(crypto.hashLength());
		long shortTag = Util.shortTag(pageTag);
		assertFalse(pool.requestedPageTags.contains(shortTag));
		pool.addPageTag(pageTag);
		assertTrue(pool.requestedPageTags.contains(shortTag));
	}
	
	@Test
	public void testAddRefTag() throws IOException {
		RefTag refTag = archive.openBlank().commit();
		assertFalse(pool.requestedRefTags.contains(refTag));
		pool.addRefTag(refTag);
		assertTrue(pool.requestedRefTags.contains(refTag));
	}
	
	@Test
	public void testAddRevision() throws IOException {
		RefTag refTag = archive.openBlank().commit();
		assertFalse(pool.requestedRevisions.contains(refTag));
		pool.addRevision(refTag);
		assertTrue(pool.requestedRevisions.contains(refTag));
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
			pool.addPageTag(tag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedPageTags);
	}
	
	@Test
	public void testAddRequestsToPeerCallsRequestRefTags() {
		LinkedList<RefTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
			tags.add(tag);
			pool.addRefTag(tag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedRefTags);
	}

	@Test
	public void testAddRequestsToPeerCallsRequestRevisionContents() {
		LinkedList<RefTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
			tags.add(tag);
			pool.addRevision(tag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedRevisions);
	}
	
	@Test
	public void testAddRequestsToPeerToleratesSeedOnly() {
		conn.mockSeedOnly = true;
		RefTag tag = new RefTag(archive, crypto.rng(archive.refTagSize()));
		pool.addRevision(tag);		
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
			pool.addPageTag(tag);
		}
		
		ByteBuffer fullTag = ByteBuffer.allocate(crypto.hashLength());
		fullTag.putLong(tags.get(3));
		fullTag.put(crypto.rng(fullTag.remaining()));
		
		archive.getStorage().write(Page.pathForTag(fullTag.array()), new byte[0]);
		pool.prune();
		assertEquals(tags.size()-1, pool.requestedPageTags.size());
		assertFalse(pool.requestedPageTags.contains(tags.get(3)));
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
			pool.addRefTag(tag);
		}
		
		pool.addRefTag(realTag);
		assertEquals(tags.size()+1, pool.requestedRefTags.size());
		pool.prune();
		assertEquals(tags, pool.requestedRefTags);
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
			pool.addRevision(tag);
		}
		
		pool.addRevision(realTag);
		assertEquals(tags.size()+1, pool.requestedRevisions.size());
		pool.prune();
		assertEquals(tags, pool.requestedRevisions);
	}
	
	@Test
	public void testPruneThreadCallsPrune() throws IOException {
		RefTag revTag = archive.openBlank().commit();
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(archive);
		pool2.addRevision(revTag);
		assertFalse(pool2.requestedRevisions.isEmpty());
		assertTrue(Util.waitUntil(RequestPool.pruneIntervalMs+10, ()->pool2.requestedRevisions.isEmpty()));
	}
	
	@Test
	public void testStopCancelsPruneThread() throws IOException {
		RefTag revTag = archive.openBlank().commit();
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(archive);
		pool2.addRevision(revTag);
		pool2.stop();
		assertFalse(pool2.requestedRevisions.isEmpty());
		assertFalse(Util.waitUntil(RequestPool.pruneIntervalMs+10, ()->pool2.requestedRevisions.isEmpty()));
	}
}
