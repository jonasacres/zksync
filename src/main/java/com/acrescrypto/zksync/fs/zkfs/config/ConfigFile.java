package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.IOException;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SecureFile;
import com.acrescrypto.zksync.fs.FS;

public abstract class ConfigFile {
	protected Key cipherKey;
	protected FS storage;
	protected boolean dirty;
	
	public ConfigFile() {
		this(null, null);
	}
	
	public ConfigFile(FS storage) {
		this(storage, null);
	}
	
	public ConfigFile(FS storage, Key cipherKey) {
		this.storage = storage;
		this.cipherKey = cipherKey;
		
		try {
			read();
		} catch(IOException e) {
			setDefaults();
		}
	}
	
	abstract public void setDefaults();
	abstract public String path();
	
	public void read() throws IOException {
		if(storage == null) throw new IOException(path() + "no storage FS supplied");
		if(cipherKey != null) {
			deserialize(SecureFile.atPath(storage, path(), cipherKey, new byte[0], null).read());
		} else {
			deserialize(storage.read(path()));
		}
	}
	
	public void write() throws IOException {
		if(storage == null) throw new IOException(path() + "no storage FS supplied");
		if(cipherKey != null) {
			SecureFile.atPath(storage, path(), cipherKey, new byte[0], null).write(serialize(), 1024);
		} else {
			storage.write(path(), serialize());
		}
		
		storage.squash(path());
		dirty = false;
	}
	
	public boolean isDirty() {
		return dirty;
	}
	
	abstract protected byte[] serialize();
	abstract protected void deserialize(byte[] serialized);
}
