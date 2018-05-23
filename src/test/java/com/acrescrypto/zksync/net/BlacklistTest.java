package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.net.Blacklist.BlacklistCallback;

public class BlacklistTest {
	static FS fs;
	Blacklist blacklist;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		fs = new RAMFS();
		if(fs.exists("/")) fs.rmrf("/");
	}
	
	@Before
	public void beforeEach() throws IOException, InvalidBlacklistException {
		CryptoSupport crypto = new CryptoSupport();
		Key key = new Key(crypto, crypto.rng(crypto.symKeyLength()));
		blacklist = new Blacklist(fs, "blacklist", key);
	}
	
	@After
	public void afterEach() throws IOException {
		if(fs.exists("/")) fs.rmrf("/");
	}
	
	@Test
	public void testAdd() throws IOException {
		assertFalse(blacklist.contains("127.0.0.1"));
		blacklist.add("127.0.0.1", 1000*60*60*24);
		assertTrue(blacklist.contains("127.0.0.1"));
	}
	
	@Test
	public void testExpiration() throws IOException {
		long timeStart = System.currentTimeMillis();
		int expireTime = 100;
		blacklist.add("127.0.0.1", expireTime);
		assertTrue(blacklist.contains("127.0.0.1"));
		
		try {
			do {
				Thread.sleep(1);
			} while(System.currentTimeMillis() <= timeStart+expireTime);
		} catch (InterruptedException e) {
		}
		
		assertFalse(blacklist.contains("127.0.0.1"));
	}
	
	@Test
	public void testPrune() throws IOException {
		long startTime = System.currentTimeMillis();
		int waitTime = 100;
		blacklist.add("127.0.0.1", waitTime);
		blacklist.prune();
		assertTrue(blacklist.blockedAddresses.containsKey("127.0.0.1"));
		while(System.currentTimeMillis() <= startTime + waitTime) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {}
		}
		blacklist.prune();
		assertFalse(blacklist.blockedAddresses.containsKey("127.0.0.1"));
	}
	
	@Test
	public void testSerialization() throws IOException, InvalidBlacklistException {
		long startTime = System.currentTimeMillis();
		for(int i = 0; i < 16; i++) {
			blacklist.add("127.0.0." + i, 1000*i);
		}
		
		int margin = (int) (System.currentTimeMillis() - startTime);
		
		Blacklist clone = new Blacklist(blacklist.fs, blacklist.path, blacklist.key);
		assertFalse(clone.contains("127.0.0.0"));
		for(int i = 1; i < 16; i++) {
			String address = "127.0.0." + i;
			assertTrue(clone.contains(address));
			int difference = (int) Math.abs(1000*i + startTime - clone.blockedAddresses.get(address).expiration);
			assertTrue(difference <= 1 + margin);
		}
	}
	
	@Test
	public void testCallback() throws IOException {
		class HoldGuy { boolean passed; }
		String address = "10.0.0.1";
		int durationMs = 1000;
		HoldGuy holder = new HoldGuy();
		blacklist.addCallback(new BlacklistCallback() {
			public void disconnectAddress(String cbAddress, int cbDurationMs) {
				assertEquals(address, cbAddress);
				assertEquals(durationMs, cbDurationMs);
				holder.passed = true;
			}
		});
		blacklist.add(address, durationMs);
		assertTrue(holder.passed);
	}
	
	@Test
	public void testRemoveCallback() throws IOException {
		class HoldGuy { boolean passed; }
		String address = "10.0.0.1";
		int durationMs = 1000;
		HoldGuy holder = new HoldGuy();
		holder.passed = true;
		BlacklistCallback callback = new BlacklistCallback() {
			public void disconnectAddress(String cbAddress, int cbDurationMs) {
				holder.passed = false;
			}
		};
		blacklist.addCallback(callback);
		blacklist.removeCallback(callback);
		blacklist.add(address, durationMs);
		assertTrue(holder.passed);
	}
	
	@Test
	public void testClearRemovesAllEntries() throws IOException {
		for(int i = 0; i < 16; i++) {
			blacklist.add("127.0.0." + i, 10000);
		}
		
		blacklist.clear();
		
		for(int i = 1; i < 16; i++) {
			assertFalse(blacklist.contains("127.0.0." + i));
		}
	}
}
