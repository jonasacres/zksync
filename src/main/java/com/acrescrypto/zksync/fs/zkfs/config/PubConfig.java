package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.StringReader;
import java.security.SecureRandom;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

// Contains information needed to decrypt archive. Stored in plaintext.
public class PubConfig extends ConfigFile {
	protected byte[] archiveId;
	public static int defaultArgon2TimeCost = 4, defaultArgon2MemoryCost = 65536, defaultArgon2Parallelism = 1;
	
	public PubConfig(FS storage) {
		super(storage);
	}

	public PubConfig() {
		super();
	}

	private int argon2MemoryCost, argon2TimeCost, argon2Parallelism;
	
	public void setDefaults() {
		setArgon2TimeCost(defaultArgon2TimeCost);
		setArgon2MemoryCost(defaultArgon2MemoryCost);
		setArgon2Parallelism(defaultArgon2Parallelism);
		
		/* TODO: There is a serious chicken-and-egg problem here.
		 * The goal is to be able to locate an archive from a human-readable string, such as a passphrase.
		 * To get there, that string has to map to an identifier, and the present (and likely correct) answer is a KDF.
		 * A PBKDF like argon2 has difficulty parameters that must scale over time, and depending on the application.
		 * But how to find difficulty parameters for existing archives?
		 * 
		 * One solution is to fix the parameters for the seed_id used to locate archives in the DHT. These parameters
		 * could be specified by the DHT itself. But who in the DHT has the power to set/change these values? Perhaps the
		 * DHT has them as constants, and a new DHT must be constructed to support new constants. But then how does one
		 * know which DHT to search for a key in?
		 * 
		 * This choice is tied to the archive itself. Since all keys are derived from the passphrase somehow, they too
		 * need these parameters set.
		 * 
		 * Perhaps a fixed set of parameters could be used to derive the seed id, which would suffice to locate nodes
		 * carrying the data, but then a tunable set (encrypted with the seed id) would be used for all the data keys.
		 * 
		 * What WON'T work is what we have now, which is making the archiveId totally random.
		 */
		archiveId = new byte[64];
		(new SecureRandom()).nextBytes(archiveId);
	}

	public String path() {
		return ZKArchive.CONFIG_DIR + "config.pub";
	}
	
	protected void deserialize(byte[] serialized) {
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		
		argon2MemoryCost = json.getInt("argon2MemoryCost");
		argon2TimeCost = json.getInt("argon2TimeCost");
		argon2Parallelism = json.getInt("argon2Parallelism");
		archiveId = Util.hexToBytes(json.getString("archiveId"));
	}
	
	protected byte[] serialize() {
		JsonObject json = Json.createObjectBuilder()
			.add("argon2MemoryCost", Integer.valueOf(argon2MemoryCost))
			.add("argon2TimeCost", Integer.valueOf(argon2TimeCost))
			.add("argon2Parallelism", Integer.valueOf(argon2Parallelism))
			.add("archiveId", Util.bytesToHex(archiveId))
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
	
	public byte[] getArchiveId() {
		return archiveId;
	}
	
	public void setArchiveId(byte[] archiveId) {
		this.archiveId = archiveId;
	}
}
