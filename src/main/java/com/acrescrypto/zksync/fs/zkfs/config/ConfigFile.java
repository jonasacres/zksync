package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.IOException;

import com.acrescrypto.zksync.crypto.Key;
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
			deserialize(cipherKey.wrappedDecrypt(storage.read(path())));
		} else {
			deserialize(storage.read(path()));
		}
	}
	
	public void write() throws IOException {
		if(storage == null) throw new IOException(path() + "no storage FS supplied");
		if(cipherKey != null) {
			storage.write(path(), cipherKey.wrappedEncrypt(serialize(), 1024));
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
