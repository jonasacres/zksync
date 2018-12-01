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

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
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
		boolean requestedAll, mockSeedOnly, setPaused, setPausedValue, requestedConfigInfo;
		int requestedPriority;
		RevisionTag requestedRevTag;
		
		LinkedList<RevisionTag> requestedRefTags = new LinkedList<>();
		LinkedList<RevisionTag> requestedRevisions = new LinkedList<>();
		LinkedList<RevisionTag> requestedRevisionDetails = new LinkedList<>();
		LinkedList<Long> requestedPageTags = new LinkedList<>();
		LinkedList<Long> requestedInodeIds = new LinkedList<>();
		
		@Override public void setPaused(boolean paused) {
			this.setPaused = true;
			this.setPausedValue = paused;
		}
		
		@Override public void requestAll() { requestedAll = true; }
		@Override public void requestPageTags(int priority, Collection<Long> pageTags) {
			requestedPriority = priority;
			requestedPageTags.addAll(pageTags);
		}
		
		@Override public void requestInodes(int priority, RevisionTag revTag, Collection<Long> inodeIds) throws PeerCapabilityException { 
			if(mockSeedOnly) throw new PeerCapabilityException();
			requestedPriority = priority;
			requestedRevTag = revTag;
			requestedInodeIds.addAll(inodeIds);
		}
		
		@Override public void requestRevisionContents(int priority, Collection<RevisionTag> refTags) throws PeerCapabilityException {
			if(mockSeedOnly) throw new PeerCapabilityException();
			requestedPriority = priority;
			requestedRevisions.addAll(refTags);
		}
		
		@Override public void requestRevisionDetails(int priority, Collection<RevisionTag> refTags) throws PeerCapabilityException {
			if(mockSeedOnly) throw new PeerCapabilityException();
			requestedPriority = priority;
			requestedRevisionDetails.addAll(refTags);
		}
		
		@Override public void requestConfigInfo() {
			requestedConfigInfo = true;
		}
	}
	
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchiveConfig config;
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
		config = ZKArchiveConfig.createDefault(accessor);
		archive = new DummyArchive(config);
		
		conn = new DummyConnection();
		pool = new RequestPool(config);
	}
	
	@After
	public void afterEach() {
		pool.stop();
		archive.close();
		config.close();
		conn.close();
		RequestPool.pruneIntervalMs = RequestPool.DEFAULT_PRUNE_INTERVAL_MS;
	}
	
	@AfterClass
	public static void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testBlank() {}
	
	@Test
	public void testAddPageTagShort() {
		assertFalse(pool.hasPageTag(12, 1234l));
		pool.addPageTag(12, 1234l);
		assertTrue(pool.hasPageTag(12, 1234l));
	}
	
	@Test
	public void testAddPageTagShortAllowsReprioritization() {
		pool.addPageTag(12, 1234l);
		pool.addPageTag(123, 1234l);
		assertFalse(pool.hasPageTag(12, 1234l));
		assertTrue(pool.hasPageTag(123, 1234l));
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
	public void testAddPageTagBytesAllowsReprioritization() {
		byte[] pageTag = crypto.rng(crypto.hashLength());
		long shortTag = Util.shortTag(pageTag);
		pool.addPageTag(19, pageTag);
		pool.addPageTag(18, pageTag);
		assertTrue(pool.hasPageTag(18, shortTag));
		assertFalse(pool.hasPageTag(19, shortTag));
	}
	
	@Test
	public void testAddInode() throws IOException {
		RevisionTag refTag = archive.openBlank().commit();
		assertFalse(pool.hasInode(271828183, refTag, InodeTable.INODE_ID_INODE_TABLE));
		pool.addInode(271828183, refTag, InodeTable.INODE_ID_INODE_TABLE);
		assertTrue(pool.hasInode(271828183, refTag, InodeTable.INODE_ID_INODE_TABLE));
	}
	
	@Test
	public void testAddInodeAllowsReprioritization() throws IOException {
		RevisionTag refTag = archive.openBlank().commit();
		pool.addInode(271828183, refTag, InodeTable.INODE_ID_INODE_TABLE);
		pool.addInode(314159265, refTag, InodeTable.INODE_ID_INODE_TABLE);
		assertFalse(pool.hasInode(271828183, refTag, InodeTable.INODE_ID_INODE_TABLE));
		assertTrue(pool.hasInode(314159265, refTag, InodeTable.INODE_ID_INODE_TABLE));
	}
	
	@Test
	public void testAddRevision() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		assertFalse(pool.hasRevision(-123456, revTag));
		pool.addRevision(-123456, revTag);
		assertTrue(pool.hasRevision(-123456, revTag));
	}
	
	@Test
	public void testAddRevisionAllowsReprioritization() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		pool.addRevision(-123456, revTag);
		pool.addRevision(-654321, revTag);
		assertFalse(pool.hasRevision(-123456, revTag));
		assertTrue(pool.hasRevision(-654321, revTag));
	}
	
	@Test
	public void testAddRevisionDetails() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		assertFalse(pool.hasRevisionDetails(-123456, revTag));
		pool.addRevisionDetails(-123456, revTag);
		assertTrue(pool.hasRevisionDetails(-123456, revTag));
	}
	
	@Test
	public void testAddRevisionDefailtsAllowsReprioritization() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		pool.addRevisionDetails(-123456, revTag);
		pool.addRevisionDetails(-654321, revTag);
		assertFalse(pool.hasRevisionDetails(-123456, revTag));
		assertTrue(pool.hasRevisionDetails(-654321, revTag));
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
	public void testAddRequestsToPeerAddsConfigInfoRequestIfRequested() {
		pool.setRequestingConfigInfo(true);
		pool.addRequestsToConnection(conn);
		assertTrue(conn.requestedConfigInfo);
	}
	
	@Test
	public void testAddRequestsToPeerDoesNotAddConfigInfoRequestIfNotRequested() {
		pool.addRequestsToConnection(conn);
		assertFalse(conn.requestedConfigInfo);
	}
	
	@Test
	public void testAddRequestsToPeerDoesNotAddDataRequestsIfConfigNotReadyToReceive() {
		LinkedList<Long> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			long tag = ByteBuffer.wrap(crypto.rng(8)).getLong();
			tags.add(tag);
			pool.addPageTag(123, tag);
		}

		config.setPageSize(-1);
		assertFalse(config.canReceive());
		pool.addRequestsToConnection(conn);
		assertEquals(0, conn.requestedPageTags.size());
	}
	
	@Test
	public void testAddRequestsToPeerCallsRequestPageTags() {
		LinkedList<Long> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			long tag = ByteBuffer.wrap(crypto.rng(8)).getLong();
			tags.add(tag);
			pool.addPageTag(123, tag);
		}
		
		pool.receivedConfigInfo();
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedPageTags);
		assertEquals(123, conn.requestedPriority);
	}
	
	@Test
	public void testAddRequestsToPeerCallsRequestInodes() {
		RefTag refTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
		RevisionTag revTag = new RevisionTag(refTag, 0, 0);
		LinkedList<Long> inodeIds = new LinkedList<>();
		
		for(int i = 0; i < 16; i++) {
			inodeIds.add(crypto.defaultPrng().getLong());
			pool.addInode(321, revTag, inodeIds.getLast());
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(inodeIds, conn.requestedInodeIds);
		assertEquals(revTag, conn.requestedRevTag);
		assertEquals(321, conn.requestedPriority);
	}

	@Test
	public void testAddRequestsToPeerCallsRequestRevisionContents() {
		LinkedList<RevisionTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
			RevisionTag revTag = new RevisionTag(tag, 0, 0);
			tags.add(revTag);
			pool.addRevision(-123, revTag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedRevisions);
		assertEquals(-123, conn.requestedPriority);
	}
	
	@Test
	public void testAddRequestsToPeerCallsRequestRevisionDetails() {
		LinkedList<RevisionTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RefTag tag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
			RevisionTag revTag = new RevisionTag(tag, 0, 0);
			tags.add(revTag);
			pool.addRevisionDetails(-123, revTag);
		}
		
		pool.addRequestsToConnection(conn);
		assertEquals(tags, conn.requestedRevisionDetails);
		assertEquals(-123, conn.requestedPriority);
	}
	
	@Test
	public void testAddRequestsToPeerToleratesSeedOnly() {
		conn.mockSeedOnly = true;
		RefTag tag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
		RevisionTag revTag = new RevisionTag(tag, 0, 0);
		pool.addRevision(0, revTag);		
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
	public void testPruneRemovesAcquiredInodes() throws IOException {
		ZKFS fs = archive.openBlank();
		fs.write("testpath", new byte[2*archive.getConfig().getPageSize()]);
		fs.commit();
		
		Inode realInode = fs.inodeForPath("testpath");
		LinkedList<Long> inodeIds = new LinkedList<>();
		
		for(int i = 0; i < 16; i++) {
			inodeIds.add(crypto.defaultPrng().getLong());
			pool.addInode(i, fs.getBaseRevision(), inodeIds.getLast());
		}
		
		pool.addInode(-1, fs.getBaseRevision(), realInode.getStat().getInodeId());
		assertTrue(pool.hasInode(-1, fs.getBaseRevision(), realInode.getStat().getInodeId()));
		pool.prune();
		
		for(int i = 0; i < inodeIds.size(); i++) {
			assertTrue(pool.hasInode(i, fs.getBaseRevision(), inodeIds.get(i)));
		}
		
		assertFalse(pool.hasInode(-1, fs.getBaseRevision(), realInode.getStat().getInodeId()));
	}
	
	@Test
	public void testPruneRemovesAcquiredRevisions() throws IOException {
		RevisionTag realTag = archive.openBlank().commit();

		LinkedList<RevisionTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RevisionTag tag;
			do {
				RefTag refTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
				tag = new RevisionTag(refTag, 0, 0);
			} while(tag.getRefTag().getRefType() == RefTag.REF_TYPE_IMMEDIATE);
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
	public void testPruneRemovesAcquiredRevisionDetails() throws IOException {
		RevisionTag realTag = archive.openBlank().commit();

		LinkedList<RevisionTag> tags = new LinkedList<>();
		for(int i = 0; i < 16; i++) {
			RevisionTag tag;
			do {
				RefTag refTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
				tag = new RevisionTag(refTag, 0, 0);
			} while(tag.getRefTag().getRefType() == RefTag.REF_TYPE_IMMEDIATE);
			tags.add(tag);
			pool.addRevisionDetails(i, tag);
		}
		
		pool.addRevisionDetails(-1, realTag);
		assertTrue(pool.hasRevisionDetails(-1, realTag));
		pool.prune();
		
		for(int i = 0; i < tags.size(); i++) {
			assertTrue(pool.hasRevisionDetails(i, tags.get(i)));
		}
		
		assertFalse(pool.hasRevisionDetails(-1, realTag));
	}
	
	@Test
	public void testPruneThreadCallsPrune() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(config);
		pool2.addRevision(0, revTag);
		assertFalse(pool2.requestedRevisions.isEmpty());
		assertTrue(Util.waitUntil(RequestPool.pruneIntervalMs+10, ()->pool2.hasRevision(0, revTag)));
		pool2.stop();
	}
	
	@Test
	public void testStopCancelsPruneThread() throws IOException {
		RevisionTag revTag = archive.openBlank().commit();
		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(config);
		pool2.addRevision(0, revTag);
		pool2.stop();
		assertFalse(pool2.requestedRevisions.isEmpty());
		assertFalse(Util.waitUntil(RequestPool.pruneIntervalMs+10, ()->!pool2.hasRevision(0, revTag)));
	}
	
	@Test
	public void testSerialization() throws IOException {
		LinkedList<RevisionTag> tags = new LinkedList<>();
		for(int i = 0; i < 64; i++) {
			RefTag refTag = new RefTag(archive, crypto.rng(archive.getConfig().refTagSize()));
			RevisionTag revTag = new RevisionTag(refTag, 0, 0);
			tags.add(revTag);
			
			if(i % 3 == 0) pool.addPageTag(i, tags.peekLast().getShortHash());
			if(i % 3 == 1) {
				for(int j = 0; j < 4; j++) {
					pool.addInode(i, tags.peekLast(), tags.peekLast().getShortHash()+j);
				}
			}
			if(i % 3 == 2) pool.addRevision(i, tags.peekLast());
		}
		
		pool.write();
		RequestPool pool2 = new RequestPool(config);
		pool2.read();
		
		assertEquals(pool.requestingEverything, pool2.requestingEverything);
		for(int i = 0; i < 64; i++) {
			if(i % 3 == 0) assertTrue(pool2.hasPageTag(i, tags.get(i).getShortHash()));
			if(i % 3 == 1) {
				for(int j = 0; j < 4; j++) {
					assertTrue(pool2.hasInode(i, tags.get(i), tags.get(i).getShortHash() + j));
				}
			}
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
		RequestPool pool2 = new RequestPool(config);
		pool2.addPageTag(0, 0);
		assertTrue(Util.waitUntil(100, ()->archive.getConfig().getLocalStorage().exists(pool2.path())));
		pool2.stop();
	}
	
	@Test
	public void testBackgroundThreadDoesNotWriteDataWhenStopped() throws IOException {
		pool.stop();

		RequestPool.pruneIntervalMs = 10;
		RequestPool pool2 = new RequestPool(config); // need a second pool for the faster prune interval
		pool2.addPageTag(0, 0);
		pool2.stop();
		try { archive.getConfig().getLocalStorage().unlink(pool2.path()); } catch(ENOENTException exc) {}
		assertFalse(Util.waitUntil(100, ()->archive.getConfig().getLocalStorage().exists(pool2.path())));
	}
	
	@Test
	public void testAutomaticallyRequestsConfigInfoWhenCanReceiveFalse() {
		config.setPageSize(-1);
		assertFalse(config.canReceive());

		RequestPool pool2 = new RequestPool(config); // need a second pool for the faster prune interval
		assertTrue(pool2.requestingConfigInfo);
		pool2.stop();		
	}
	
	@Test
	public void testDoesNotAutomaticallyRequestConfigInfoWhenCanReceiveTrue() {
		RequestPool pool2 = new RequestPool(config); // need a second pool for the faster prune interval
		assertFalse(pool2.requestingConfigInfo);
		pool2.stop();		
	}
}
