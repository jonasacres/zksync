package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

// TODO: having IOException thrown here is a necessary evil... or is it?
public class HashCache<K,V> {
	public interface CacheLookup<K,V> {
		public V getValue(K key) throws IOException;
	}

	public interface CacheEvict<K,V> {
		public void evict(K key, V value) throws IOException;
	}

	protected HashMap<K,V> cache = new HashMap<K,V>();
	protected Queue<K> evictionQueue = new LinkedList<K>();
	protected int capacity;
	
	CacheLookup<K,V> lookup;
	CacheEvict<K,V> evict;
	
	public HashCache(int capacity, CacheLookup<K,V> lookup, CacheEvict<K,V> evict) {
		this.capacity = capacity;
		this.lookup = lookup;
		this.evict = evict;
	}
	
	public synchronized V get(K key) throws IOException {
		V result = cache.getOrDefault(key, null);
		if(result == null) result = add(key);
		resetKey(key);
		return result;
	}
		
	protected synchronized V add(K key) throws IOException {
		V result = lookup.getValue(key);
		if(result == null) return null;
		
		cache.put(key, result);
		enforceCapacityLimit();
		return result;
	}
	
	public synchronized V remove(K key) throws IOException {
		if(!cache.containsKey(key)) return null;
		V value = cache.get(key);
		evictionQueue.remove(key);
		evict.evict(key, value);
		return value;
	}
	
	public synchronized void removeAll() throws IOException {
		// TODO: better hope we don't get an ioexception on the second element or later, or we will have an inconsistent state 
		for(K key : cache.keySet()) {
			evict.evict(key, cache.get(key));
		}
		
		cache.clear();
	}
	
	protected void resetKey(K key) {
		evictionQueue.remove(key);
		evictionQueue.add(key);
	}
	
	public synchronized Collection<V> values() {
		return cache.values();
	}
	
	protected void enforceCapacityLimit() throws IOException {
		if(capacity <= 0) return;
		while(cache.size() > capacity) {
			K key = evictionQueue.remove();
			V value = cache.remove(key);
			evict.evict(key, value);
		}
	}
	
	public Iterable<K> cachedKeys() {
		return cache.keySet();
	}
}
