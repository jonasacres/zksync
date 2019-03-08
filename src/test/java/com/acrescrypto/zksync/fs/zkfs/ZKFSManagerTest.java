package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.utility.Util;

public class ZKFSManagerTest {
	final static String TESTDIR = "zksync-test/zkfsmanagertest";
	
	ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	ZKFSManager manager;
	LocalFS mirrorFs;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		fs = archive.openBlank();
		manager = new ZKFSManager(fs);
		
		try(LocalFS lfs = new LocalFS("/tmp")) {
			lfs.mkdirp(TESTDIR);
		}
		
		mirrorFs = new LocalFS("/tmp/" + TESTDIR);
	}
	
	@After
	public void afterEach() throws IOException {
		manager.close();
		if(!fs.isClosed()) fs.close();
		archive.close();
		master.close();
		mirrorFs.purge();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test
	public void testConstructorAddsMonitorToZKFS() {
		assertTrue(fs.dirtyMonitors.contains(manager.fsMonitor));
	}

	@Test
	public void testConstructorAddsMonitorToRevisionList() {
		assertTrue(archive.getConfig().getRevisionList().monitors.contains(manager.revMonitor));
	}
	
	@Test
	public void testCloseRemovesMonitorFromZKFS() throws IOException {
		manager.close();
		assertFalse(fs.dirtyMonitors.contains(manager.fsMonitor));
	}
	
	@Test
	public void testCloseRemovesMonitorFromRevisionList() throws IOException {
		manager.close();
		assertFalse(archive.getConfig().getRevisionList().monitors.contains(manager.revMonitor));
	}
	
	@Test
	public void testAutocommitDisabledByDefault() {
		assertFalse(manager.isAutocommiting());
	}
	
	@Test
	public void testAutofollowDisabledByDefault() {
		assertFalse(manager.isAutofollowing());
	}
	
	@Test
	public void testFsChangesTriggerAutocommitTimerWhenAutocommitEnabled() throws IOException {
		int interval = 100;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		
		assertTrue(Util.waitUntil(2*interval, ()->!fs.isDirty()));
	}
	
	@Test
	public void testFsChangesResetAutocommitTimerWhenAutocommitEnabled() throws IOException {
		// TODO ITF: UniversalTests 3/3/19 a13698c+
		int interval = 100;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		Util.sleep(interval - 20);
		long timeStart = System.currentTimeMillis();
		fs.write("file", "somebytes2".getBytes());
		
		assertTrue(Util.waitUntil(2*interval, ()->!fs.isDirty()));
		long duration = System.currentTimeMillis() - timeStart;
		assertTrue(duration >= interval);
	}
	
	@Test
	public void testFsChangesDoNotTriggerAutocommitTimerWhenAutocommitDisabled() throws IOException {
		int interval = 100;
		manager.setAutocommit(false);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		
		assertFalse(Util.waitUntil(2*interval, ()->!fs.isDirty()));
	}
	
	@Test
	public void testAutocommitIntervalResetsTimerIfNewIntervalSpecified() throws IOException {
		int interval1 = 100, interval2 = 200;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval1);
		fs.write("file", "somebytes".getBytes());
		manager.setAutocommitIntervalMs(interval2);
		
		long timeStart = System.currentTimeMillis();
		assertTrue(Util.waitUntil(2*interval2, ()->!fs.isDirty()));
		long duration = System.currentTimeMillis() - timeStart;
		assertTrue(duration >= interval2);
	}
	
	@Test
	public void testAutocommitIntervalDoesNotResetTimerIfExistingIntervalSpecified() throws IOException {
		int interval = 100;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		Util.sleep(interval - 20);
		manager.setAutocommitIntervalMs(interval);
		
		long timeStart = System.currentTimeMillis();
		assertTrue(Util.waitUntil(2*interval, ()->!fs.isDirty()));
		long duration = System.currentTimeMillis() - timeStart;
		assertTrue(duration < interval);
	}
	
	@Test
	public void testSetAutomergeTrueSetsRevisionListAutomergeToTrue() {
		manager.setAutomerge(true);
		assertTrue(archive.getConfig().getRevisionList().automerge);
	}

	@Test
	public void testSetAutomergeFalseSetsRevisionListAutomergeToFalse() {
		manager.setAutomerge(false);
		assertFalse(archive.getConfig().getRevisionList().automerge);
	}

	@Test
	public void testSetAutocommitTrueStartsTimerIfTimerWasNotPreviouslyRunning() throws IOException {
		int interval = 100;
		manager.setAutocommit(false);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		assertFalse(Util.waitUntil(2*interval, ()->!fs.isDirty()));
		
		manager.setAutocommit(true);
		assertTrue(Util.waitUntil(2*interval, ()->!fs.isDirty()));
	}
	
	@Test
	public void testSetAutocommitTrueDoesNotAffectTimerIfTimerWasPreviouslyRunning() throws IOException {
		int interval = 100;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		
		Util.sleep(interval - 20);
		manager.setAutocommit(true);
		long timeStart = System.currentTimeMillis();
		assertTrue(Util.waitUntil(2*interval, ()->!fs.isDirty()));
		long duration = System.currentTimeMillis() - timeStart;
		assertTrue(duration < interval);
	}
	
	@Test
	public void testSetAutocommitFalseCancelsTimerIfTimerWasPreviouslyRunning() throws IOException {
		int interval = 100;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		manager.setAutocommit(false);
		
		assertFalse(Util.waitUntil(2*interval, ()->!fs.isDirty()));
	}
	
	@Test
	public void testSetAutocommitReturnsGracefullyIfTimerWasNotPreviouslyRunning() throws IOException {
		int interval = 100;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		manager.setAutocommit(false);
		manager.setAutocommit(false);
	}
	
	@Test
	public void testNewRevisionsAreCheckedOutWhenAutofollowEnabledAndNoChangesPendingAndBaseRevisionBlank() throws IOException {
		manager.setAutofollow(true);
		try(ZKFS fs2 = archive.openBlank()) {
			RevisionTag tag = fs2.commit();
			assertTrue(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
		}
	}
	
	@Test
	public void testNewRevisionsAreCheckedOutWhenAutofollowEnabledAndNoChangesPendingAndBaseRevisionNotBlank() throws IOException {
		RevisionTag tag = archive.openBlank().commitAndClose();
		manager.fs.rebase(tag);
		manager.setAutofollow(true);
		RevisionTag tag2 = tag.getFS().commitAndClose();
		assertTrue(Util.waitUntil(100, ()->fs.baseRevision.equals(tag2)));
	}
	
	@Test
	public void testNewRevisionsAreNotCheckedOutWhenAutofollowEnabledAndChangesArePending() throws IOException {
		manager.setAutofollow(true);
		fs.write("change", "somedata".getBytes());
		try(ZKFS fs2 = archive.openBlank()) {
			RevisionTag tag = fs2.commit();
			assertFalse(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
		}
	}
	
	@Test
	public void testNewRevisionsAreNotCheckedOutWhenNewRevisionNotDescendentOfCurrentRevision() throws IOException {
		RevisionTag tag = archive.openBlank().commitAndClose();
		manager.fs.rebase(tag);
		manager.setAutofollow(true);
		RevisionTag tag2 = archive.openBlank().commitAndClose();
		assertFalse(Util.waitUntil(100, ()->fs.baseRevision.equals(tag2)));
	}
	
	@Test
	public void testNewRevisionsAreNotCheckedOutWhenAutofollowDisabled() throws IOException {
		manager.setAutofollow(false);
		try(ZKFS fs2 = archive.openBlank()) {
			RevisionTag tag = fs2.commit();
			assertFalse(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
		}
	}
	
	@Test
	public void testSetAutomirrorFalseDisablesWatchIfRunning() throws IOException {
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(true);
		assertTrue(manager.mirror.isWatching());
		
		manager.setAutomirror(false);
		assertFalse(manager.mirror.isWatching());
	}
	
	@Test
	public void testSetAutomirrorFalseDoesNotFreakOutIfWatchNotRunning() throws IOException {
		manager.setAutomirror(false);
		
		// and again, with an automirror path set
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(false);
	}

	@Test(expected=EINVALException.class)
	public void testSetAutomirrorTrueThrowsExceptionIfAutomirrorPathNotSet() throws IOException {
		manager.setAutomirror(true);
	}
	
	@Test
	public void testSetAutomirrorTrueStartsWatchIfAutomirrorPathSet() throws IOException {
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(true);
		assertTrue(manager.mirror.isWatching());
	}
	
	@Test
	public void testSetAutomirrorTrueDoesNotFreakOutIfWatchRunning() throws IOException {
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(true);
		assertTrue(manager.mirror.isWatching());
		
		manager.setAutomirror(true);
		assertTrue(manager.mirror.isWatching());
	}
	
	@Test
	public void testSetAutomirrorPathNullStopsWatchIfRunning() throws IOException {
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(true);
		assertTrue(manager.mirror.isWatching());
		
		FSMirror mirror = manager.mirror;
		manager.setAutomirrorPath(null);
		assertFalse(manager.isAutomirroring());
		assertFalse(mirror.isWatching());
	}
	
	@Test
	public void testSetAutomirrorPathDoesNotStartWatchIfNotAlreadyRunning() throws IOException {
		manager.setAutomirrorPath(mirrorFs.getRoot());
		assertFalse(manager.isAutomirroring());
		assertFalse(manager.mirror.isWatching());
	}
	
	@Test
	public void testSetAutomirrorPathRestartsWatchIfPreviouslyRunning() throws IOException {
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(true);
		FSMirror mirror = manager.mirror;
		
		manager.setAutomirrorPath(mirrorFs.getRoot());
		assertNotEquals(mirror, manager.mirror);
		assertFalse(mirror.isWatching());
		assertTrue(manager.isAutomirroring());
		assertTrue(manager.mirror.isWatching());
	}
	
	@Test
	public void testSyncsArchiveToMirrorTargetIfAutofollowEnabled() throws IOException {
		manager.setAutofollow(true);
		manager.setAutomirrorPath(mirrorFs.getRoot());

		try(ZKFS fs2 = archive.openBlank()) {
			fs2.write("file", "somebytes".getBytes());
			RevisionTag tag = fs2.commit();
			
			assertTrue(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
			assertTrue(Util.waitUntil(100, ()->mirrorFs.exists("file")));
			assertArrayEquals("somebytes".getBytes(), mirrorFs.read("file"));
		}
	}

	@Test
	public void testDoesNotSyncArchiveToMirrorTargetIfAutofollowDisabled() throws IOException {
		manager.setAutofollow(false);
		manager.setAutomirrorPath(mirrorFs.getRoot());

		try(ZKFS fs2 = archive.openBlank()) {
			fs2.write("file", "somebytes".getBytes());
			fs2.commit();
		}
		
		assertFalse(Util.waitUntil(100, ()->mirrorFs.exists("file")));
	}

	@Test
	public void testSyncsArchiveToMirrorTargetIfAutomirrorPathNotSet() throws IOException {
		manager.setAutofollow(true);
		manager.setAutomirrorPath(null);

		try(ZKFS fs2 = archive.openBlank()) {
			fs2.write("file", "somebytes".getBytes());
			RevisionTag tag = fs2.commit();
			assertTrue(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
			assertFalse(Util.waitUntil(100, ()->mirrorFs.exists("file")));
		}
	}
	
	@Test
	public void testSerializationAndDeserialization() throws IOException {
		manager.setAutofollow(true);
		manager.setAutocommit(true);
		manager.setAutomerge(true);
		manager.setAutocommitIntervalMs(123456);
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(true);
		byte[] serialized = manager.serialize();
		
		ZKFSManager deserialized = new ZKFSManager(archive.getConfig(), serialized);
		assertEquals(manager.isAutofollowing(), deserialized.isAutofollowing());
		assertEquals(manager.isAutocommiting(), deserialized.isAutocommiting());
		assertEquals(manager.isAutomerging(), deserialized.isAutomerging());
		assertEquals(manager.isAutomirroring(), deserialized.isAutomirroring());
		assertEquals(manager.getAutocommitIntervalMs(), deserialized.getAutocommitIntervalMs());
		assertEquals(manager.getAutomirrorPath(), deserialized.getAutomirrorPath());
		deserialized.close();
	}
	
	@Test
	public void testPersistentManagerReadsExisting() throws IOException {
		RevisionTag notLatest = manager.fs.commit();
		manager.fs.commit();
		manager.fs.rebase(notLatest);

		manager.setAutofollow(true);
		manager.setAutocommit(true);
		manager.setAutomerge(true);
		manager.setAutocommitIntervalMs(123456);
		manager.setAutomirrorPath(mirrorFs.getRoot());
		manager.setAutomirror(true);
		manager.write();
		
		try(ZKFSManager persistent = new ZKFSManager(archive.getConfig())) {
			assertEquals(manager.isAutofollowing(), persistent.isAutofollowing());
			assertEquals(manager.isAutocommiting(), persistent.isAutocommiting());
			assertEquals(manager.isAutomerging(), persistent.isAutomerging());
			assertEquals(manager.isAutomirroring(), persistent.isAutomirroring());
			assertEquals(manager.getAutocommitIntervalMs(), persistent.getAutocommitIntervalMs());
			assertEquals(manager.getAutomirrorPath(), persistent.getAutomirrorPath());
			assertEquals(notLatest, persistent.fs.getBaseRevision());
		}
	}

	@Test
	public void testPersistentManagerInitializesFromDefault() throws IOException {
		RevisionTag latest = archive.openBlank().commitAndClose();
		
		try(ZKFSManager persistent = new ZKFSManager(archive.getConfig())) {
			assertFalse(persistent.isAutofollowing());
			assertFalse(persistent.isAutocommiting());
			assertFalse(persistent.isAutomirroring());
			assertEquals(0, persistent.getAutocommitIntervalMs());
			assertNull(persistent.getAutomirrorPath());
			assertEquals(latest, persistent.fs.getBaseRevision());
		}
	}
}
