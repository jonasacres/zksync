package com.acrescrypto.zksync.fs.zkfs.config;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import com.acrescrypto.zksync.utility.Util;

public class SectionedBuffer {
	protected HashMap<String,byte[]> records = new HashMap<>();
	int totalBytes;
	
	public SectionedBuffer() {
		totalBytes = 4;
	}
	
	public SectionedBuffer(byte[] serialized) {
		deserialize(serialized);
	}
	
	public SectionedBuffer addRecord(String name, byte[] contents) {
		records.put(name, contents);
		totalBytes += 1 + name.getBytes().length + 4 + contents.length;
		return this;
	}
	
	public SectionedBuffer addRecord(String name, String contents) {
		return addRecord(name, contents.getBytes());
	}
	
	public SectionedBuffer addRecord(String name, int value) {
		return addRecord(name, Util.serializeInt(value));
	}
	
	public SectionedBuffer addRecord(String name, long value) {
		return addRecord(name, Util.serializeLong(value));
	}
	
	public byte[] serialize() {
		List<String> keys = new ArrayList<String>(records.keySet());
		Collections.sort(keys);

		ByteBuffer buf = ByteBuffer.allocate(totalBytes);
		buf.putInt(records.size());
		
		for(String key : keys) {
			byte[] contents = records.get(key);
			
			buf.put((byte) key.getBytes().length);
			buf.put(key.getBytes());
			buf.putInt(contents.length);
			buf.put(contents);
		}
		
		return buf.array();
	}
	
	public SectionedBuffer deserialize(byte[] serialized) {
		totalBytes = serialized.length;
		records.clear();
		
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		int numSections = buf.getInt();
		
		for(int i = 0; i < numSections; i++) {
			byte nameLen = buf.get();
			byte[] nameBytes = new byte[nameLen];
			buf.get(nameBytes);
			String name = new String(nameBytes);
			
			int contentsLen = buf.getInt();
			byte[] contents = new byte[contentsLen];
			buf.get(contents);
			
			records.put(name, contents);
		}
		
		return this;
	}
	
	public Collection<String> keys() {
		return records.keySet();
	}
	
	public byte[] contentForKey(String key) {
		return records.get(key);
	}
	
	public byte[] contentForKey(String key, byte[] defaultValue) {
		if(!records.containsKey(key)) {
			return defaultValue;
		}
		
		return records.get(key);
	}
}
