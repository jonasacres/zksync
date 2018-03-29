package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

/* Store information specific to this user's copy of the archive; not shared with peers. Encrypted, though peers
 * would have knowledge of key if they did possess encrypted LocalConfig file.
 */
public class LocalConfig extends ConfigFile {
	private boolean squashIds;
	private int fileMode = 0644, directoryMode = 0755;
	private int uid = 0, gid = 0;
	private String user = "root", group = "root";
	
	public LocalConfig(FS storage, Key cipherKey) {
		super(storage, cipherKey);
	}
	
	public void setDefaults() {
		setSquashIds(true);
	}
	
	public String path() {
		return ZKArchive.LOCAL_DIR + "config.local";
	}
	
	protected void deserialize(byte[] serialized) {
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		squashIds = json.getBoolean("squashIds");
	}
	
	protected byte[] serialize() {
		JsonObject json = Json.createObjectBuilder()
			.add("squashIds", Boolean.valueOf(squashIds))
			.add("fileMode", Integer.valueOf(fileMode))
			.add("directoryMode", Integer.valueOf(directoryMode))
			.add("uid", Integer.valueOf(uid))
			.add("gid", Integer.valueOf(gid))
			.add("user", user)
			.add("group", group)
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

	public int getFileMode() {
		return fileMode;
	}

	public void setFileMode(int fileMode) {
		this.fileMode = fileMode;
	}

	public int getDirectoryMode() {
		return directoryMode;
	}

	public void setDirectoryMode(int directoryMode) {
		this.directoryMode = directoryMode;
	}

	public int getGid() {
		return gid;
	}

	public void setGid(int gid) {
		this.gid = gid;
	}

	public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	public String getGroup() {
		return group;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}
}
