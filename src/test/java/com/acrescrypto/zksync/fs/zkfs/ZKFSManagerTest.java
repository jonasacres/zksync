package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.utility.Util;

public class ZKFSManagerTest {
	ZKMaster master;
	ZKArchive archive;
	ZKFS fs;
	ZKFSManager manager;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		fs = archive.openBlank();
		manager = new ZKFSManager(fs);
	}
	
	@After
	public void afterEach() throws IOException {
		manager.close();
		fs.close();
		archive.close();
		master.close();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		ZKFSTest.restoreArgon2Costs();
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
	public void testCloseRemovesMonitorFromZKFS() {
		manager.close();
		assertFalse(fs.dirtyMonitors.contains(manager.fsMonitor));
	}
	
	@Test
	public void testCloseRemovesMonitorFromRevisionList() {
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
		int interval = 100;
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(interval);
		fs.write("file", "somebytes".getBytes());
		Util.sleep(interval - 20);
		fs.write("file", "somebytes2".getBytes());
		
		long timeStart = System.currentTimeMillis();
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
	public void testNewRevisionsAreCheckedOutWhenAutofollowEnabledAndNoChangesPending() throws IOException {
		manager.setAutofollow(true);
		ZKFS fs2 = archive.openBlank();
		RevisionTag tag = fs2.commit();
		assertTrue(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
	}
	
	@Test
	public void testNewRevisionsAreNotCheckedOutWhenAutofollowEnabledAndChangesArePending() throws IOException {
		manager.setAutofollow(true);
		fs.write("change", "somedata".getBytes());
		ZKFS fs2 = archive.openBlank();
		RevisionTag tag = fs2.commit();
		assertFalse(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
	}
	
	@Test
	public void testNewRevisionsAreNotCheckedOutWhenAutofollowDisabled() throws IOException {
		manager.setAutofollow(false);
		ZKFS fs2 = archive.openBlank();
		RevisionTag tag = fs2.commit();
		assertFalse(Util.waitUntil(100, ()->fs.baseRevision.equals(tag)));
	}
}