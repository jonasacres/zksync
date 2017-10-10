package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

/* Store information specific to this user's copy of the archive; not shared with peers. Encrypted, though peers
 * would have knowledge of key if they did possess encrypted LocalConfig file.
 */
public class LocalConfig extends ConfigFile {
	private boolean squashIds;
	
	public LocalConfig(FS storage, Key cipherKey) {
		super(storage, cipherKey);
	}
	
	public void setDefaults() {
		setSquashIds(true);
	}
	
	public String path() {
		return ZKFS.LOCAL_DIR + "config.local";
	}
	
	protected void deserialize(byte[] serialized) {
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		squashIds = json.getBoolean("squashIds");
	}
	
	protected byte[] serialize() {
		JsonObject json = Json.createObjectBuilder()
			.add("squashIds", Boolean.valueOf(squashIds))
			.build();
		return json.toString().getBytes();
	}

	public boolean getSquashIds() {
		return squashIds;
	}

	public void setSquashIds(boolean squashIds) {
		if(squashIds == this.squashIds) return;
		this.squashIds = squashIds;
		this.dirty = true;
	}
}
