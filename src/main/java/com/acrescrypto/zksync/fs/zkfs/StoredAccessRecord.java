package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.utility.Util;

public class StoredAccessRecord implements AutoCloseable {
	private ZKArchiveConfig config;
	private ArchiveAccessor accessor;
	private Key writeKey;
	protected byte[] archiveId;
	protected boolean locallyInstantiated;
	protected int accessLevel;
	
	public StoredAccessRecord(ZKArchiveConfig config, int accessLevel) {
		this.config = config;
		this.archiveId = config.getArchiveId();
		this.accessLevel = accessLevel;
	}
	
	public StoredAccessRecord(ZKMaster master, ByteBuffer buf) throws IOException {
		deserialize(master, buf);
	}
	
	public void close() {
		if(locallyInstantiated) config.close();
	}
	
	public synchronized ZKArchiveConfig getConfig() throws IOException {
		if(config == null) {
			// TODO API: (coverage) branch
			config = ZKArchiveConfig.openExisting(accessor, archiveId, true, writeKey);
		}
		
		return config;
	}
	
	protected byte[] serialize() {
		boolean writeSeedKey = accessLevel >= StoredAccess.ACCESS_LEVEL_SEED;
		boolean writeReadKey = accessLevel >= StoredAccess.ACCESS_LEVEL_READ && !config.getAccessor().isSeedOnly();
		boolean writeWriteKey = accessLevel >= StoredAccess.ACCESS_LEVEL_READWRITE && !config.isReadOnly();
		
		return Util.concat(
				Util.serializeInt(0), // reserved
				Util.serializeInt(accessLevel),
				keyIfDesired(writeSeedKey, config.accessor.seedRoot),
				keyIfDesired(writeReadKey, config.accessor.passphraseRoot),
				keyIfDesired(writeWriteKey, config.writeRoot),
				config.archiveId
				);
	}
	
	protected byte[] keyIfDesired(boolean desired, Key key) {
		if(!desired || key == null) return new byte[config.getCrypto().symKeyLength()];
		return key.getRaw();
	}
	
	protected boolean isBlank(byte[] array) {
		for(byte b : array) {
			if(b != 0) return false;
		}
		
		return true;
	}
	
	protected void deserialize(ZKMaster master, ByteBuffer buf) throws IOException {
		int version = buf.getInt();
		if(version != 0) throw new EINVALException("unsupported version");
		
		accessLevel = buf.getInt();
		if(accessLevel > StoredAccess.ACCESS_LEVEL_READWRITE) throw new EINVALException("unsupported record type");
		
		byte[] seedKeyRaw = new byte[master.crypto.symKeyLength()];
		buf.get(seedKeyRaw);
		
		byte[] passphraseKeyRaw = new byte[master.crypto.symKeyLength()];
		buf.get(passphraseKeyRaw);
		
		byte[] writeKeyRaw = new byte[master.crypto.symKeyLength()];
		buf.get(writeKeyRaw);
		
		archiveId = new byte[master.crypto.hashLength()];
		buf.get(archiveId);
		
		Key seedKey = new Key(master.crypto, seedKeyRaw);
		Key passphraseKey = isBlank(passphraseKeyRaw) ? null : new Key(master.crypto, passphraseKeyRaw);
		
		writeKey = isBlank(writeKeyRaw) ? null : new Key(master.crypto, writeKeyRaw);
		boolean seedOnly = accessLevel <= StoredAccess.ACCESS_LEVEL_SEED || isBlank(passphraseKeyRaw);
		accessor = master.makeAccessorForRoot(seedOnly ? seedKey : passphraseKey, seedOnly);
		config = ZKArchiveConfig.openExisting(accessor, archiveId, false, writeKey);
		if(config.haveConfigLocally()) {
			config.finishOpening();
		}
		
		locallyInstantiated = true;
	}
	
	public byte[] getArchiveId() {
		return archiveId;
	}
	
	public int getAccessLevel() {
		return accessLevel;
	}
}
