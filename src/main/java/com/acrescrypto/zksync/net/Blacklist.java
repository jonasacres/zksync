package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.utility.Util;

public class Blacklist {
	public final static int DEFAULT_BLACKLIST_DURATION_MS = 1000*60*3; // peers are blocked for 3 hours for suspicious traffic
	protected HashMap<String,BlacklistEntry> blockedAddresses = new HashMap<String,BlacklistEntry>();
	protected FS fs;
	protected String path;
	protected Key key;
	protected final Logger logger = LoggerFactory.getLogger(Blacklist.class);
	protected LinkedList<BlacklistCallback> callbacks = new LinkedList<BlacklistCallback>();
	protected boolean enabled;
	
	protected interface BlacklistCallback {
		void disconnectAddress(String address, long durationMs);
	}
	
	public Blacklist(FS fs, String path, Key key) throws IOException, InvalidBlacklistException {
		this.fs = fs;
		this.path = path;
		this.key = key;
		this.enabled = true;
		read();
	}
	
	public synchronized void clear() throws IOException {
		blockedAddresses.clear();
		write();
	}
	
	public synchronized void write() throws IOException {
		byte[] serialized = serialize();
		int blockSize = 65536, padLen = (int) (blockSize*Math.ceil(((double) serialized.length)/blockSize));
		MutableSecureFile.atPath(fs, path, key).write(serialize(), padLen);
	}
	
	public void add(String address, long durationMs) throws IOException {
		add(new BlacklistEntry(address, durationMs));
	}

	public void addWithAbsoluteTime(String address, long expTime) throws IOException {
		// TODO API: (test) test addWithAbsoluteTime
		BlacklistEntry entry = BlacklistEntry.withExpiration(address, expTime);
		add(entry);
	}
	
	public void add(BlacklistEntry entry) throws IOException {
		Collection<BlacklistCallback> callbackList;
		long durationMs = entry.getExpiration() - System.currentTimeMillis();

		synchronized(this) {
			BlacklistEntry existing = blockedAddresses.getOrDefault(entry.getAddress(), null);
			if(existing != null) {
				logger.warn("Renewing blacklist entry for {} for {}ms", existing.getAddress(), durationMs);
				existing.update(durationMs);
			} else {
				logger.warn("Adding blacklist entry for {} for {}ms", entry.getAddress(), durationMs);
				blockedAddresses.put(entry.getAddress(), entry);
			}
			
			write();
			
			callbackList = new LinkedList<>(callbacks);
		}
		
		for(BlacklistCallback callback : callbackList) {
			callback.disconnectAddress(entry.getAddress(), durationMs);
		}
	}

	public synchronized void addCallback(BlacklistCallback callback) {
		callbacks.add(callback);
	}
	
	public synchronized void removeCallback(BlacklistCallback callback) {
		callbacks.remove(callback);
	}
	
	public synchronized ArrayList<BlacklistEntry> allEntries() {
		return new ArrayList<BlacklistEntry>(blockedAddresses.values());
	}
	
	public void read() throws IOException, InvalidBlacklistException {
		try {
			deserialize(MutableSecureFile.atPath(fs, path, key).read());
		} catch(ENOENTException exc) {
		} catch(SecurityException exc) {
			throw new InvalidBlacklistException();
		}
	}
	
	public synchronized boolean contains(String address) {
		BlacklistEntry entry = blockedAddresses.getOrDefault(address, null);
		if(entry == null) return false;
		if(!entry.isExpired()) return true;
		
		blockedAddresses.remove(entry.getAddress());
		return false;
	}
	
	public synchronized void prune() {
		LinkedList<String> toRemove = new LinkedList<>();
		for(String address : blockedAddresses.keySet()) {
			if(blockedAddresses.get(address).isExpired()) {
				toRemove.add(address);
			}
		}
		
		for(String address : toRemove) {
			blockedAddresses.remove(address);
		}
	}
	
	public synchronized byte[] serialize() {
		int len = 1;
		prune();
		
		// TODO API: (implement) Test enabled serialization/deserialization
		for(BlacklistEntry entry : blockedAddresses.values()) {
			len += entry.serialize().length;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(len);
		buf.put(enabled ? (byte) 1 : (byte) 0);
		for(BlacklistEntry entry : blockedAddresses.values()) {
			buf.put(entry.serialize());
		}
		
		return buf.array();
	}
	
	public void deserialize(byte[] serialized) throws InvalidBlacklistException {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		blockedAddresses.clear();
		enabled = buf.get() != 0;
		while(buf.hasRemaining()) {
			assertState(buf.remaining() >= 10);
			buf.position(buf.position() + 8);
			int entryLen = 8 + 2 + Util.unsignShort(buf.getShort());
			buf.position(buf.position()-10);
			
			assertState(buf.remaining() >= entryLen);
			byte[] serializedEntry = new byte[entryLen];
			buf.get(serializedEntry);
			BlacklistEntry entry = new BlacklistEntry(serializedEntry);
			if(entry.isExpired()) continue;
			blockedAddresses.put(entry.getAddress(), entry);
		}
	}
	
	public void assertState(boolean state) throws InvalidBlacklistException {
		if(!state) throw new InvalidBlacklistException();
	}
	
	public FS getFS() {
		return fs;
	}

	// TODO API: (test) Test Blacklist get/remove
	public BlacklistEntry get(String ip) {
		return blockedAddresses.get(ip);
	}
	
	public boolean remove(String ip) throws IOException {
		boolean removed = blockedAddresses.remove(ip) != null;
		write();
		return removed;
	}
	
	public boolean isEnabled() {
		return enabled;
	}
	
	public void setEnabled(boolean enabled) throws IOException {
		if(this.enabled == enabled) return;
		this.enabled = enabled;
		write();
	}
}
