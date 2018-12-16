package com.acrescrypto.zksync.utility;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;

public class BandwidthMonitorTest {
	BandwidthMonitor monitor;
	
	@Before
	public void beforeEach() {
		Util.setCurrentTimeMillis(0);
		monitor = new BandwidthMonitor(50, 1000);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeMillis(-1);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testGetBytesPerSecondReportsZeroWhenNoTrafficObserved() {
		assertEquals(0, monitor.getBytesPerSecond(), 0);
	}
	
	@Test
	public void testGetBytesInIntervalReportsZeroWhenNoTrafficObserved() {
		assertEquals(0, monitor.getBytesInInterval());
	}
	
	@Test
	public void testGetBytesPerSecondReportsBandwidthInBytesPerSecond() {
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(999);
		assertEquals(1000.0*1024/999.0, monitor.getBytesPerSecond(), 2);
	}
	
	@Test
	public void testGetBytesInIntervalReportsRecordedBytes() {
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(999);
		assertEquals(1024, monitor.getBytesInInterval());
	}
	
	@Test
	public void testGetBytesPerSecondAveragesBytesAcrossInterval() {
		for(int i = 0; i < 9; i++) {
			Util.setCurrentTimeMillis(100*i);
			monitor.observeTraffic(100);
		}
		
		double expected = 900 / (double) Util.currentTimeMillis() * 1000.0;
		assertEquals(expected, monitor.getBytesPerSecond(), 1e-5);
	}
	
	@Test
	public void testGetBytesInIntervalTotalsAcrossInterval() {
		for(int i = 0; i < 9; i++) {
			Util.setCurrentTimeMillis(100*i);
			monitor.observeTraffic(100);
		}
		
		assertEquals(900, monitor.getBytesInInterval());
	}
	
	@Test
	public void testGetBytesPerSecondOnlyCountsBytesInInterval() {
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(999);
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(1001);
		
		assertEquals(1024.0, monitor.getBytesPerSecond(), 1e-5);
	}
	
	@Test
	public void testGetBytesInIntervalOnlyCountsBytesInInterval() {
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(999);
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(1001);
		
		assertEquals(1024, monitor.getBytesInInterval());
	}
	
	@Test
	public void testReportsObservedTrafficToParentMonitors() {
		BandwidthMonitor p1 = new BandwidthMonitor(50, 1000), p2 = new BandwidthMonitor(50, 1000);
		monitor.addParent(p1);
		monitor.addParent(p2);
		
		Util.setCurrentTimeMillis(100);
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(1050);
		
		assertEquals(1024, p1.getBytesInInterval());
		assertEquals(1024, p2.getBytesInInterval());
		assertEquals(1024.0, p1.getBytesPerSecond(), 1e-5);
		assertEquals(1024.0, p2.getBytesPerSecond(), 1e-5);
	}
	
	@Test
	public void testRemoveParentStopsTrafficObservationReportsToParent() {
		BandwidthMonitor p1 = new BandwidthMonitor(50, 1000), p2 = new BandwidthMonitor(50, 1000);
		monitor.addParent(p1);
		monitor.addParent(p2);
		
		Util.setCurrentTimeMillis(100);
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(200);
		monitor.removeParent(p2);
		monitor.observeTraffic(1024);
		Util.setCurrentTimeMillis(1050);
		
		assertEquals(2048, p1.getBytesInInterval());
		assertEquals(1024, p2.getBytesInInterval());
		assertEquals(2048.0, p1.getBytesPerSecond(), 1e-5);
		assertEquals(1024.0, p2.getBytesPerSecond(), 1e-5);
	}
	
	@Test
	public void testObserveTrafficReturnsByteCount() {
		for(long i : new long[] { 0, 1, Long.MAX_VALUE }) {
			assertEquals(i, monitor.observeTraffic(i));
		}
	}
}
