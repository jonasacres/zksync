package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

public class HashCache<K,V> {
	public interface CacheLookup<K,V> {
		// not wild about having the IOException, an alternative would be nice
		public V getValue(K key) throws IOException;
	}

	public interface CacheEvict<K,V> {
		public void evict(K key, V value) throws IOException;
	}

	protected ConcurrentHashMap<K,V> cache = new ConcurrentHashMap<K,V>();
	protected Queue<K> evictionQueue = new LinkedList<K>();
	protected int capacity;
	
	CacheLookup<K,V> lookup;
	CacheEvict<K,V> evict;
	
	public HashCache(int capacity, CacheLookup<K,V> lookup, CacheEvict<K,V> evict) {
		this.capacity = capacity;
		this.lookup = lookup;
		this.evict = evict;
	}
	
	public HashCache(HashCache<K,V> existing, CacheLookup<K,V> lookup, CacheEvict<K,V> evict) {
		this(existing.capacity, lookup, evict);
		cache = new ConcurrentHashMap<>(existing.cache);
		evictionQueue = new LinkedList<>(existing.evictionQueue);
	}
	
	public synchronized V get(K key) throws IOException {
		V result = cache.getOrDefault(key, null);
		if(result == null) {
			result = add(key);
		} else {
			resetKey(key);
		}
		return result;
	}
	
	protected V add(K key) throws IOException {
		V result = lookup.getValue(key);
		if(result == null) return null;
		return add(key, result);
	}
	
	public synchronized V add(K key, V value) throws IOException {
		cache.put(key, value);
		resetKey(key);
		enforceCapacityLimit();
		return value;
	}
	
	public synchronized V remove(K key) throws IOException {
		if(!cache.containsKey(key)) return null;
		V value = cache.get(key);
		evictionQueue.remove(key);
		evict.evict(key, value);
		cache.remove(key);
		return value;
	}
	
	public synchronized void removeAll() throws IOException {
		LinkedList<K> toRemove = new LinkedList<>();
		try {
			for(K key : cache.keySet()) {
				evict.evict(key, cache.get(key));
				toRemove.add(key);
			}
			
			cache.clear();
			evictionQueue.clear();
		} catch(IOException exc) {
			// remove the stuff we successfully evicted before re-raising the exception
			for(K key : toRemove) {
				cache.remove(key);
				evictionQueue.remove(key);
			}
			
			throw exc;
		}
	}
	
	protected void resetKey(K key) {
		if(!cache.containsKey(key)) return;
		evictionQueue.remove(key);
		evictionQueue.add(key);
	}
	
	public synchronized Collection<V> values() {
		return cache.values();
	}
	
	protected void enforceCapacityLimit() throws IOException {
		if(capacity <= 0) return;
		while(evictionQueue.size() > capacity) {
			K key = evictionQueue.remove();
			V value = cache.get(key);
			evict.evict(key, value);
			cache.remove(key);
		}
	}
	
	public Iterable<K> cachedKeys() {
		return cache.keySet();
	}

	public boolean hasCached(K key) {
		return cache.containsKey(key);
	}

	public int cachedSize() {
		return cache.size();
	}
	
	public synchronized void setCapacity(int capacity) throws IOException {
		this.capacity = capacity;
		enforceCapacityLimit();
	}
	
	public int getCapacity() {
		return capacity;
	}
}
