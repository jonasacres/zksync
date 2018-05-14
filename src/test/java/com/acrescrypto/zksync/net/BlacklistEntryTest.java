package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class BlacklistEntryTest {
	@Test
	public void testConstructor() {
		String address = "127.0.0.1";
		int durationMs = 100;
		BlacklistEntry entry = new BlacklistEntry(address, durationMs);
		assertEquals(address, entry.address);
		assertTrue(entry.expiration >= System.currentTimeMillis() + durationMs - 1);
		assertTrue(entry.expiration <= System.currentTimeMillis() + durationMs);
	}
	
	@Test
	public void testSerialization() {
		BlacklistEntry entry = new BlacklistEntry("127.0.0.1", 100);
		BlacklistEntry deserialized = new BlacklistEntry(entry.serialize());
		assertEquals(entry.address, deserialized.address);
		assertEquals(entry.expiration, deserialized.expiration);
	}
	
	@Test
	public void testIsExpired() {
		BlacklistEntry entry = new BlacklistEntry("127.0.0.1", 2);
		assertFalse(entry.isExpired());
		
		while(System.currentTimeMillis() < entry.expiration) {
			try {
				Thread.sleep(2);
			} catch(InterruptedException exc) {}
		}
		
		assertTrue(entry.isExpired());
	}
	
	@Test
	public void testUpdate() {
		BlacklistEntry entry = new BlacklistEntry("127.0.0.1", 1);
		entry.update(100);
		assertTrue(entry.expiration >= System.currentTimeMillis() + 99);
		assertTrue(entry.expiration <= System.currentTimeMillis() + 100);
	}
	
	@Test
	public void testUpdateToForever() {
		BlacklistEntry entry = new BlacklistEntry("127.0.0.1", 1);
		entry.update(Integer.MAX_VALUE);
		assertEquals(Long.MAX_VALUE, entry.expiration);
	}
}
