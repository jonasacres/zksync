package com.acrescrypto.zksync.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedList;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PRNG;

public class HashCacheTest {
	public class CacheTestObject {
		boolean evicted;
		Integer key;
		public CacheTestObject(Integer key) { this.key = key; }
		public void evict() { evicted = true; }
		public boolean isEvicted() { return evicted; }
	}
	
	HashCache<Integer,CacheTestObject> cache;
	LinkedList<CacheTestObject> objects = new LinkedList<>();
	int cacheCapacity = 8;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() {
		cache = new HashCache<>(cacheCapacity,
			(key)->{
				CacheTestObject obj = new CacheTestObject(key);
				synchronized(this) {
					objects.add(obj);
				}
				return obj;
			},
			(key, obj)->{
				obj.evict();
			}
		);
	}
	
	@After
	public void afterEach() throws IOException {
		assertValidState();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	public void assertValidState() throws IOException {
		HashSet<Integer> cachedKeys = new HashSet<>();
		
		for(CacheTestObject object : objects) {
			if(object.isEvicted()) {
				assertNotEquals(object, cache.cache.getOrDefault(object.key, null));
			} else {
				assertFalse(cachedKeys.contains(object.key));
				cachedKeys.add(object.key);
				assertTrue(cache.hasCached(object.key));
				assertTrue(object == cache.get(object.key));
				assertTrue(cache.evictionQueue.contains(object.key));
			}
		}
		
		int numCachedKeys = 0;
		for(Integer key : cache.cachedKeys()) {
			numCachedKeys++;
			assertTrue(cachedKeys.contains(key));
		}
		
		assertEquals(numCachedKeys, cachedKeys.size());
		assertEquals(cache.cachedSize(), cachedKeys.size());
		assertEquals(cache.cachedSize(), cache.evictionQueue.size());
		
		assertEquals(cacheCapacity, cache.getCapacity());
		assertTrue(cache.cachedSize() <= cache.getCapacity());
	}
	
	@Test
	public void testInitWithCapacitySetsCapacity() {
		assertEquals(cacheCapacity, cache.getCapacity());
	}
	
	@Test
	public void testGetNonexistentKeyCreatesObject() throws IOException {
		CacheTestObject obj = cache.get(1234);
		assertEquals(1234, obj.key.intValue());
		assertFalse(obj.isEvicted());
	}
	
	@Test
	public void testGetExistingKeyReturnsSameObject() throws IOException {
		CacheTestObject obj = cache.get(1234);
		assertTrue(obj == cache.get(1234));
	}
	
	@Test
	public void testRemoveEvictsObjectFromCache() throws IOException {
		CacheTestObject obj = cache.get(1234);
		assertFalse(obj.isEvicted());
		cache.remove(1234);
		assertTrue(obj.isEvicted());
		assertFalse(cache.hasCached(1234));
	}
	
	@Test
	public void testRemoveAllEvictsAllObjectsFromCache() throws IOException {
		LinkedList<CacheTestObject> objs = new LinkedList<>();
		for(int i = 0; i < cacheCapacity; i++) {
			objs.add(cache.get(i));
		}
		
		cache.removeAll();
		assertEquals(0, cache.cachedSize());
		
		for(CacheTestObject obj : objs) {
			assertTrue(obj.isEvicted());
		}
	}
	
	@Test
	public void testAddEvictsObjectsWhenCapacityHit() throws IOException {
		LinkedList<CacheTestObject> objs = new LinkedList<>();
		for(int i = 0; i < 2*cacheCapacity; i++) {
			objs.add(cache.get(i));
		}
		
		for(CacheTestObject obj : objs) {
			assertEquals(obj.key < cacheCapacity, obj.isEvicted());
		}
		
		assertEquals(cacheCapacity, cache.cachedSize());
	}
	
	@Test
	public void testLeastRecentlyAccessedObjectsAreEvictedFirst() throws IOException {
		for(int i = 0; i < cacheCapacity; i++) {
			cache.get(i);
		}
		
		for(int i = cacheCapacity; i >= 0; i--) {
			cache.get(i);
		}
		
		for(int i = cacheCapacity; i < 2*cacheCapacity; i++) {
			int victim = 2*cacheCapacity - i - 1;
			assertTrue(cache.hasCached(victim));
			cache.get(i);
			assertFalse(cache.hasCached(victim));
		}
		
		assertEquals(cacheCapacity, cache.cachedSize());
	}
	
	@Test
	public void testHashCacheReturnsTrueIfKeyInCache() throws IOException {
		cache.get(4321);
		assertTrue(cache.hasCached(4321));
	}

	@Test
	public void testHashCacheReturnsFalseIfKeyInCache() throws IOException {
		assertFalse(cache.hasCached(4321));
	}
	
	@Test
	public void testHashCacheReturnsFalseIfKeyEvicted() throws IOException {
		for(int i = 0; i <= cacheCapacity; i++) {
			cache.get(i);
		}
		
		assertFalse(cache.hasCached(0));
	}
	
	@Test
	public void testHashCacheDownsizesWhenCapacityReduces() throws IOException {
		for(int i = 0; i < cacheCapacity; i++) {
			cache.get(i);
		}
		
		int newCapacity = cacheCapacity/2;
		cache.setCapacity(newCapacity);
		
		assertEquals(newCapacity, cache.getCapacity());
		assertEquals(newCapacity, cache.cachedSize());
		
		for(int i = 0; i < cacheCapacity; i++) {
			boolean shouldHave = i >= cacheCapacity - newCapacity;
			assertEquals(shouldHave, cache.hasCached(i));
		}
		
		cacheCapacity = newCapacity;
	}
	
	@Test
	public void testHashCacheDoesNotEvictWhenCapacityIncreased() throws IOException {
		int newCapacity = 2*cacheCapacity;
		
		for(int i = 0; i < cacheCapacity; i++) {
			cache.get(i);
		}
		
		cache.setCapacity(newCapacity);
		assertEquals(cacheCapacity, cache.cachedSize());
		for(int i = 0; i < cacheCapacity; i++) {
			assertTrue(cache.hasCached(i));
		}
		
		cacheCapacity = newCapacity;
	}
	
	@Test
	public void testCachedKeysReturnsListOfAllCachedKeys() throws IOException {
		for(int i = 0; i < cacheCapacity; i++) {
			cache.get(i);
		}
		
		int numCached = 0;
		HashSet<Integer> seen = new HashSet<>();
		for(Integer key : cache.cachedKeys()) {
			numCached++;
			assertTrue(0 <= key && key <= cacheCapacity);
			assertFalse(seen.contains(key));
			seen.add(key);
		}
		
		assertEquals(cacheCapacity, numCached);
	}
	
	@Test
	public void testCachedKeysDoesNotIncludeEvictedKeys() throws IOException {
		for(int i = 2*cacheCapacity; i >= 0; i--) {
			cache.get(i);
		}
		
		int numCached = 0;
		HashSet<Integer> seen = new HashSet<>();
		for(Integer key : cache.cachedKeys()) {
			numCached++;
			assertTrue(0 <= key && key <= cacheCapacity);
			assertFalse(seen.contains(key));
			seen.add(key);
		}
		
		assertEquals(cacheCapacity, numCached);
	}
	
	@Test
	public void testConcurrency() throws IOException {
		int numThreads = 16;
		int durationMs = 1000;
		final long deadline = System.currentTimeMillis() + durationMs;
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		
		LinkedList<Thread> threads = new LinkedList<>();
		
		for(int i = 0; i < numThreads; i++) {
			final int ii = i;
			Thread t = new Thread(()->{
				PRNG prng = crypto.prng(crypto.symNonce(ii));
				while(System.currentTimeMillis() < deadline) {
					try {
						cache.add(prng.getInt());
					} catch (IOException exc) {
						exc.printStackTrace();
						fail();
					}
				}
			});
			
			threads.add(t);
			t.start();
		}
		
		assertTrue(Util.waitUntil(durationMs + 500, ()->{
			for(Thread t : threads) {
				if(t.isAlive()) return false;
			}
			
			return true;
		}));
		
		assertValidState();
	}
}
