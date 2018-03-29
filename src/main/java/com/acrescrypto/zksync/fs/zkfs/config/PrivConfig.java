package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.IOException;
import java.io.StringReader;
import java.util.UUID;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SecureFile;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

public class PrivConfig extends ConfigFile {
	private int pageSize, immediateThreshold;
	private byte[] archiveId;
	
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
		deserialize(SecureFile.atPath(storage, path(), cipherKey, new byte[0], null).read());
	}
	
	public void write() throws IOException {
		SecureFile.atPath(storage, path(), cipherKey, new byte[0], null).write(serialize(), 1024);
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
