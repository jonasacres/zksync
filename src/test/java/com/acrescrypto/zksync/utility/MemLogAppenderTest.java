package com.acrescrypto.zksync.utility;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.utility.MemLogAppender.LogEvent;
import com.acrescrypto.zksync.utility.MemLogAppender.MemLogMonitor;

import ch.qos.logback.classic.Level;

public class MemLogAppenderTest {
	class TestMonitor implements MemLogMonitor {
		LinkedList<LogEvent> entries = new LinkedList<>();
		
		@Override
		public void receivedEntry(LogEvent entry) {
			entries.add(entry);
		}
	}
	
	static Integer originalHistoryDepth = null, originalThreshold = null;
	ConfigFile config;
	Logger logger = LoggerFactory.getLogger(MemLogAppenderTest.class);
	MemLogAppender memlog;
	TestMonitor monitor;
	
	@BeforeClass
	public static void beforeAll() {
	}
	
	@Before
	public void beforeEach() throws IOException {
		config = new ConfigFile(new RAMFS(), "config.json");
		memlog = MemLogAppender.sharedInstance();
		monitor = new TestMonitor();

		if(originalHistoryDepth == null) {
			originalHistoryDepth = memlog.getHistoryDepth();
			originalThreshold = memlog.getThreshold();
		}
		
		memlog.hardPurge();
	}
	
	@After
	public void afterEach() {
		memlog.setHistoryDepth(originalHistoryDepth);
		memlog.setThreshold(originalThreshold);
		memlog.clearMonitors();
		memlog.hardPurge();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testMemLogGetsLogEvents() {
		logger.debug("test");
		Collection<LogEvent> entries = memlog.getEntries(-1, 1, Integer.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
		assertEquals(1, entries.size());
		for(LogEvent event : entries) {
			assertEquals("test", event.getEntry().getMessage());
		}
	}
	
	@Test
	public void testMemLogIssuesSequentialIds() {
		int count = 10;
		for(int j = 0; j < count; j++) {
			logger.debug(""+j);
		}
		
		Collection<LogEvent> entries = memlog.getEntries(-1, count, Integer.MIN_VALUE, Long.MIN_VALUE, Long.MAX_VALUE);
		int i = 0;
		for(LogEvent event : entries) {
			assertEquals(i, event.getEntryId());
			assertEquals(""+i, event.getEntry().getMessage());
			i++;
		}
	}
	
	@Test
	public void testGetRecordsWithNegativeOffsetReadsFromEnd() {
		for(int i = 1; i <= 50; i++) {
			logger.debug("test " + i);
		}
		
		LinkedList<LogEvent> list = memlog.getEntries(-1, 10);
		int idx = 41;
		for(LogEvent event : list) {
			assertEquals("test " + (idx++), event.getEntry().getMessage());
		}
	}
	
	@Test
	public void testGetRecordsWithThresholdReturnsOnlyEntriesAtOrAboveThreshold() {
		logger.debug("debug");
		logger.info("info");
		logger.warn("warn");
		
		LinkedList<LogEvent> list = memlog.getEntries(-1, 10, Level.INFO_INT, Long.MIN_VALUE, Long.MAX_VALUE);
		assertEquals(2, list.size());
		assertEquals("info", list.remove().getEntry().getMessage());
		assertEquals("warn", list.remove().getEntry().getMessage());
	}
	
	@Test
	public void testGetRecordsWithBeforeIdReturnsOnlyEntriesBelowId() {
		int count = 10, threshold = 2;
		for(int i = 0; i < count; i++) {
			logger.debug("" + i);
		}
		
		LinkedList<LogEvent> list = memlog.getEntries(-1, 10, Integer.MIN_VALUE, Long.MIN_VALUE, threshold);
		assertEquals(threshold, list.size());
		for(int i = 0; i < threshold; i++) {
			assertEquals(i, list.remove().entryId);
		}
	}

	@Test
	public void testGetRecordsWithAfterIdReturnsOnlyEntriesAboveId() {
		int count = 10, threshold = 2;
		for(int i = 0; i < count; i++) {
			logger.debug("" + i);
		}
		
		LinkedList<LogEvent> list = memlog.getEntries(-1, 10, Integer.MIN_VALUE, threshold, Long.MAX_VALUE);
		assertEquals(count-threshold-1, list.size());
		for(int i = threshold+1; i < count; i++) {
			assertEquals(i, list.remove().entryId);
		}
	}

	@Test
	public void testMemlogPrunesToHistoryDepth() {
		memlog.setHistoryDepth(16);
		for(int i = 1; i <= memlog.getHistoryDepth(); i++) {
			logger.debug("test " + i);
			assertEquals(Math.min(memlog.getHistoryDepth(), i), memlog.numEntries());
		}
	}
	
	@Test
	public void testMemlogDropsOlderEntriesForNew() {
		memlog.setHistoryDepth(16);
		for(int i = 1; i <= memlog.getHistoryDepth(); i++) {
			logger.debug("test " + i);
		}
		
		assertEquals("test " + memlog.getHistoryDepth(),
				memlog.getEntries(1).getFirst().getEntry().getMessage());
	}
	
	@Test
	public void testSetHistoryDepthPrunesToNewSize() {
		memlog.setHistoryDepth(16);
		for(int i = 1; i <= memlog.getHistoryDepth(); i++) {
			logger.debug("test " + i);
		}
		
		assertEquals(memlog.getHistoryDepth(), memlog.numEntries());		
		
		memlog.setHistoryDepth(8);
		assertEquals(8, memlog.getHistoryDepth());
		assertEquals(memlog.getHistoryDepth(), memlog.numEntries());
	}
	
	@Test
	public void testSetThresholdPrunesToNewThreshold() {
		logger.debug("debug");
		logger.warn("warn");
		
		assertEquals(2, memlog.numEntries());
		memlog.setThreshold(Level.WARN_INT);
		assertEquals(1, memlog.numEntries());
		assertEquals(Level.WARN_INT, memlog.getEntries(1).getFirst().getEntry().getLevel().toInt());
	}
	
	@Test
	public void testMonitorsReceiveEntries() {
		memlog.addMonitor(monitor);
		logger.debug("hello world");
		
		assertEquals(1, monitor.entries.size());
		assertEquals("hello world", monitor.entries.getFirst().getEntry().getMessage());
	}
	
	@Test
	public void testRemoveMonitorStopsMessagesToMonitor() {
		memlog.addMonitor(monitor);
		memlog.removeMonitor(monitor);
		logger.debug("hello world");
		assertEquals(0, monitor.entries.size());
	}
	
	@Test
	public void testRemoveMonitorDoesNotAffectOtherMonitors() {
		TestMonitor monitor2 = new TestMonitor();
		
		memlog.addMonitor(monitor);
		memlog.addMonitor(monitor2);
		memlog.removeMonitor(monitor);
		
		logger.debug("hello world");
		
		assertEquals(0, monitor.entries.size());
		assertEquals(1, monitor2.entries.size());
	}
	
	@Test
	public void testClearMonitorsRemovesAllMonitors() {
		memlog.addMonitor(monitor);
		memlog.clearMonitors();
		logger.debug("hello world");
		assertEquals(0, monitor.entries.size());
	}
}
