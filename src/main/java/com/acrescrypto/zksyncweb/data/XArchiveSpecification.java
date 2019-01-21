package com.acrescrypto.zksyncweb.data;

public class XArchiveSpecification {
	private String writePassphrase;
	private String readPassphrase;

	private byte[] writeKey;
	private byte[] readKey;
	private byte[] seedKey;
	
	private Integer pageSize;
	private String description;
	
	private byte[] archiveId;
	private Integer savedAccessLevel;
	
	public String getWritePassphrase() {
		return writePassphrase;
	}
	
	public void setWritePassphrase(String writePassphrase) {
		this.writePassphrase = writePassphrase;
	}
	
	public String getReadPassphrase() {
		return readPassphrase;
	}
	
	public void setReadPassphrase(String readPassphrase) {
		this.readPassphrase = readPassphrase;
	}
	
	public byte[] getWriteKey() {
		return writeKey;
	}
	
	public void setWriteKey(byte[] writeKey) {
		this.writeKey = writeKey;
	}
	
	public byte[] getReadKey() {
		return readKey;
	}
	
	public void setReadKey(byte[] readKey) {
		this.readKey = readKey;
	}
	
	public byte[] getSeedKey() {
		return seedKey;
	}
	
	public void setSeedKey(byte[] seedKey) {
		this.seedKey = seedKey;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public byte[] getArchiveId() {
		return archiveId;
	}

	public void setArchiveId(byte[] archiveId) {
		this.archiveId = archiveId;
	}

	public Integer getSavedAccessLevel() {
		return savedAccessLevel;
	}

	public void setSavedAccessLevel(Integer savedAccessLevel) {
		this.savedAccessLevel = savedAccessLevel;
	}
}
