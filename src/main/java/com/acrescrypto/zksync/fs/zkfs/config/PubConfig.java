package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

// Contains information needed to decrypt archive. Stored in plaintext.
public class PubConfig extends ConfigFile {
	
	public PubConfig(FS storage) {
		super(storage);
	}

	public PubConfig() {
		super();
	}

	private int argon2MemoryCost, argon2TimeCost, argon2Parallelism;
	
	public void setDefaults() {
		setArgon2MemoryCost(65536);
		setArgon2TimeCost(4);
		setArgon2Parallelism(1);
	}

	public String path() {
		return ZKFS.CONFIG_DIR + "config.pub";
	}
	
	protected void deserialize(byte[] serialized) {
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		
		argon2MemoryCost = json.getInt("argon2MemoryCost");
		argon2TimeCost = json.getInt("argon2TimeCost");
		argon2Parallelism = json.getInt("argon2Parallelism");
	}
	
	protected byte[] serialize() {
		JsonObject json = Json.createObjectBuilder()
			.add("argon2MemoryCost", Integer.valueOf(argon2MemoryCost))
			.add("argon2TimeCost", Integer.valueOf(argon2TimeCost))
			.add("argon2Parallelism", Integer.valueOf(argon2Parallelism))
			.build();
		return json.toString().getBytes();
	}
	
	public int getArgon2Parallelism() {
		return argon2Parallelism;
	}

	public void setArgon2Parallelism(int argon2Parallelism) {
		if(argon2Parallelism == this.argon2Parallelism) return;
		this.argon2Parallelism = argon2Parallelism;
		this.dirty = true;
	}

	public int getArgon2TimeCost() {
		return argon2TimeCost;
	}

	public void setArgon2TimeCost(int argon2TimeCost) {
		if(argon2TimeCost == this.argon2TimeCost) return;
		this.argon2TimeCost = argon2TimeCost;
		this.dirty = true;
	}

	public int getArgon2MemoryCost() {
		return argon2MemoryCost;
	}

	public void setArgon2MemoryCost(int argon2MemoryCost) {
		if(argon2MemoryCost == this.argon2MemoryCost) return;
		this.argon2MemoryCost = argon2MemoryCost;
		this.dirty = true;
	}
}
