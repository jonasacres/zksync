package com.acrescrypto.zksync.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.mutable.MutableBoolean;

import com.acrescrypto.zksync.utility.BandwidthAllocator.BandwidthAllocation;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BandwidthAllocatorTest {
	BandwidthAllocator allocator;
	
	@Before
	public void beforeEach() {
		allocator = new BandwidthAllocator(500000, 2);
		Util.setCurrentTimeMillis(0);
	}
	
	@After
	public void afterEach() {
		Util.setCurrentTimeMillis(-1);
	}
	
	@Test
	public void testConstructorWithoutIntervalSetsDefaultInterval() {
		assertEquals(BandwidthAllocator.DEFAULT_ALLOCATION_INTERVAL_MS, new BandwidthAllocator(1).getAllocationIntervalMs());
	}
	
	@Test
	public void testConstructorSetsAllocationInterval() {
		BandwidthAllocator example = new BandwidthAllocator(1000, 10);
		assertEquals(10, example.getAllocationIntervalMs());
	}
	
	@Test
	public void testConstructorSetsRate() {
		BandwidthAllocator example = new BandwidthAllocator(1234, 10);
		assertEquals(1234.0, example.getBytesPerSecond(), 0);
	}
	
	@Test
	public void testPendingAllocationsReceiveEqualPortions() {
		BandwidthAllocation[] allocations = new BandwidthAllocation[10];
		Long[] sizes = new Long[allocations.length];
		allocator.requestAllocation().requestBytes(Long.MAX_VALUE); // drain the pool; now everyone else has to block
		for(int i = 0; i < allocations.length; i++) {
			final int ii = i;
			allocations[i] = allocator.requestAllocation();
			new Thread(()->sizes[ii] = allocations[ii].requestBytes(Long.MAX_VALUE)).start();
		}
		
		Util.setCurrentTimeMillis(allocator.getReallocationTime());
		for(int i = 0; i < allocations.length; i++) {
			final int ii = i;
			// TODO Urgent: (itf) ba549c3 linux UniversalTests 2018-12-12, AssertionError
			assertTrue(Util.waitUntil(100, ()->sizes[ii] != null));
			assertEquals(sizes[0], sizes[i]);
		}
		
		assertEquals(allocator.bytesPerInterval()/allocations.length, sizes[0].longValue());
	}
	
	@Test
	public void testPendingAllocationsDoNotReceiveMoreThanRequestedAmount() {
		BandwidthAllocation[] allocations = new BandwidthAllocation[10];
		Long[] sizes = new Long[allocations.length];
		int amount = 10;
		
		allocator.requestAllocation().requestBytes(Long.MAX_VALUE); // drain the pool; now everyone else has to block
		for(int i = 0; i < allocations.length; i++) {
			final int ii = i;
			allocations[i] = allocator.requestAllocation();
			new Thread(()->sizes[ii] = allocations[ii].requestBytes(amount)).start();
		}
		
		Util.setCurrentTimeMillis(allocator.getReallocationTime());
		for(int i = 0; i < allocations.length; i++) {
			final int ii = i;
			assertTrue(Util.waitUntil(100, ()->sizes[ii] != null));
			assertEquals(sizes[0], sizes[i]);
		}
		
		assertEquals(amount, sizes[0].longValue());
	}
	
	@Test
	public void testRequestBytesReturnsRequestedAmountIfUnlimited() {
		long amount = 1122334455667788990L;
		allocator.setBytesPerSecond(Double.POSITIVE_INFINITY);
		assertEquals(amount, allocator.requestAllocation().requestBytes(amount));
	}
	
	@Test
	public void testRequestBytesReturnsAvailableBytesIfRequestExceedsAvailability() {
		MutableBoolean allocated = new MutableBoolean(false);
		BandwidthAllocation allocation = allocator.requestAllocation();
		new Thread(()->{
			allocation.expect(1000);
			allocated.setTrue();
		}).start();
		
		Util.waitUntil(100, ()->allocated.isTrue());
		assertEquals(1000, allocation.requestBytes(1001));
	}
	
	@Test
	public void testRequestBytesReturnsRequestSizeIfRequestedBytesAreAvailable() {
		// TODO Urgent: (itf) Test stalls. 2018-12-07 496c588 AllTests Linux
		MutableBoolean allocated = new MutableBoolean(false);
		BandwidthAllocation allocation = allocator.requestAllocation();
		new Thread(()->{
			allocation.expect(1000);
			allocated.setTrue();
		}).start();
		
		Util.waitUntil(100, ()->allocated.isTrue());
		assertEquals(900, allocation.requestBytes(900));
	}
	
	@Test
	public void testRequestBytesDeductsBytesFromAvailability() {
		MutableBoolean allocated = new MutableBoolean(false);
		BandwidthAllocation allocation = allocator.requestAllocation();
		new Thread(()->{
			allocation.expect(1000);
			allocated.setTrue();
		}).start();
		
		Util.waitUntil(100, ()->allocated.isTrue());
		allocation.requestBytes(900);
		assertEquals(100, allocation.getBytesRemaining());
	}
	
	@Test
	public void testRequestBytesBlocksUntilBytesAvailable() {
		allocator.requestAllocation().requestBytes(Long.MAX_VALUE);
		MutableBoolean allocated = new MutableBoolean(false);
		BandwidthAllocation allocation = allocator.requestAllocation();
		new Thread(()->{
			allocation.requestBytes(1);
			allocated.setTrue();
		}).start();
		
		assertFalse(Util.waitUntil(10, ()->allocated.isTrue()));
		Util.setCurrentTimeMillis(allocator.getReallocationTime());
		assertTrue(Util.waitUntil(10, ()->allocated.isTrue()));
	}
	
	@Test
	public void testRequestBytesBlocksIfAllocationExpired() {
		MutableBoolean allocated = new MutableBoolean(false);
		BandwidthAllocation allocation = allocator.requestAllocation();
		allocation.expect(1);
		Util.setCurrentTimeMillis(allocation.getExpirationTime());
		allocator.requestAllocation().requestBytes(Long.MAX_VALUE);
		
		new Thread(()->{
			allocation.requestBytes(1);
			allocated.setTrue();
		}).start();
		
		assertFalse(Util.waitUntil(10, ()->allocated.isTrue()));
	}
	
	@Test
	public void testGetBytesRemainingReturnsZeroIfAllocationExpired() {
		BandwidthAllocation allocation = allocator.requestAllocation();
		allocation.expect(1);
		Util.setCurrentTimeMillis(allocation.getExpirationTime());
		assertEquals(0, allocation.getBytesRemaining());
	}
}
