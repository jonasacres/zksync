package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.IOException;
import java.io.StringReader;
import java.util.UUID;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

public class PrivConfig extends ConfigFile {
	private int pageSize, immediateThreshold;
	private byte[] archiveId;
	
	// TODO: consider allowing an entry for a retired key, to allow recovery of an interrupted rekey operation
	
	// TODO: option for user/group info preferences: none, numeric id, string id, both w/ string pref, both w/ id pref. default to "none"
	
	public PrivConfig(FS storage, Key cipherKey) {
		super(storage, cipherKey);
	}
	
	public void setDefaults() {
		setPageSize(65536);
		setImmediateThreshold(64);
		setArchiveId(UUID.randomUUID().toString().getBytes());
	}
	
	public String path() {
		return ZKArchive.CONFIG_DIR + "config.priv";
	}
	
	public void read() throws IOException {
		deserialize(cipherKey.wrappedDecrypt(storage.read(path())));
	}
	
	public void write() throws IOException {
		storage.write(path(), cipherKey.wrappedEncrypt(serialize(), 1024));
		storage.squash(path());
	}
	
	protected void deserialize(byte[] serialized) {
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		
		pageSize = json.getInt("pageSize");
		immediateThreshold = json.getInt("immediateThreshold");
		archiveId = json.getString("archiveId").getBytes();
	}
	
	protected byte[] serialize() {
		JsonObject json = Json.createObjectBuilder()
			.add("pageSize", Integer.valueOf(pageSize))
			.add("immediateThreshold", Integer.valueOf(immediateThreshold))
			.add("archiveId", new String(archiveId))
			.build();
		return json.toString().getBytes();
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		if(pageSize == this.pageSize) return;
		this.pageSize = pageSize;
		this.dirty = true;
	}

	// TODO: Kill this. It should be hashLength-1. Also, make sure all uses of this imply an immediate iff length < hashLength.
	public int getImmediateThreshold() {
		return immediateThreshold;
	}

	public void setImmediateThreshold(int immediateThreshold) {
		this.immediateThreshold = immediateThreshold;
	}

	public byte[] getArchiveId() {
		return archiveId;
	}

	public void setArchiveId(byte[] archiveId) {
		this.archiveId = archiveId;
	}	
}
