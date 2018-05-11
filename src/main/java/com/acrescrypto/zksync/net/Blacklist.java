package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.InvalidBlacklistException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.utility.Util;

public class Blacklist {
	protected HashMap<String,BlacklistEntry> blockedAddresses = new HashMap<String,BlacklistEntry>();
	protected FS fs;
	protected String path;
	protected Key key;
	protected final Logger logger = LoggerFactory.getLogger(Blacklist.class);
	protected LinkedList<BlacklistCallback> callbacks = new LinkedList<BlacklistCallback>();
	
	protected interface BlacklistCallback {
		void blacklistedAddress(String address);
	}
	
	public Blacklist(FS fs, String path, Key key) {
		this.fs = fs;
		this.path = path;
		this.key = key;
	}
	
	
	public void write() throws IOException {
		byte[] serialized = serialize();
		int blockSize = 65536, padLen = (int) (blockSize*Math.ceil(((double) serialized.length)/blockSize));
		MutableSecureFile.atPath(fs, path, key).write(serialize(), padLen);
	}
	
	public void add(String address, int durationMs) {
		BlacklistEntry entry = blockedAddresses.getOrDefault(address, null);
		if(entry != null) {
			logger.warn("Renewing blacklist entry for {} for {}ms", address, durationMs);
			entry.update(durationMs);
		} else {
			logger.warn("Adding blacklist entry for {} for {}ms", address, durationMs);
			entry = new BlacklistEntry(address, durationMs);
			blockedAddresses.put(address, entry);
		}
	}
	
	public void addCallback(BlacklistCallback callback) {
		callbacks.add(callback);
	}
	
	public void removeCallback(BlacklistCallback callback) {
		callbacks.remove(callback);
	}
	
	public void read() throws IOException, InvalidBlacklistException {
		deserialize(MutableSecureFile.atPath(fs, path, key).read());
	}
	
	public boolean contains(String address) {
		BlacklistEntry entry = blockedAddresses.getOrDefault(address, null);
		if(entry == null) return false;
		if(!entry.isExpired()) return true;
		
		blockedAddresses.remove(entry.address);
		return false;
	}
	
	public void prune() {
		for(String address : blockedAddresses.keySet()) {
			if(blockedAddresses.get(address).isExpired()) {
				blockedAddresses.remove(address);
			}
		}
	}
	
	public byte[] serialize() {
		int len = 0;
		prune();
		for(BlacklistEntry entry : blockedAddresses.values()) {
			len += entry.serialize().length;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(len);
		for(BlacklistEntry entry : blockedAddresses.values()) {
			buf.put(entry.serialize());
		}
		
		return buf.array();
	}
	
	public void deserialize(byte[] serialized) throws InvalidBlacklistException {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
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
			blockedAddresses.put(entry.address, entry);
		}
	}
	
	public void assertState(boolean state) throws InvalidBlacklistException {
		if(!state) throw new InvalidBlacklistException();
	}
}
