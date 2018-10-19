package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.utility.Util;

public class StoredAccessRecord {
	private ZKArchive archive;
	private ArchiveAccessor accessor;
	private Key writeKey;
	protected byte[] archiveId;
	protected boolean seedOnly, locallyInstantiated;
	
	public StoredAccessRecord(ZKArchive archive, boolean seedOnly) {
		this.seedOnly = seedOnly;
		this.archive = archive;
	}
	
	public StoredAccessRecord(ZKMaster master, ByteBuffer buf) throws IOException {
		deserialize(master, buf);
	}
	
	public void close() {
		if(locallyInstantiated) archive.close();
	}
	
	public synchronized ZKArchive getArchive() throws IOException {
		if(archive == null) {
			ZKArchiveConfig config = ZKArchiveConfig.openExisting(accessor, archiveId, true, writeKey);
			archive = config.getArchive();
		}
		return archive;
	}
	
	protected byte[] serialize() {
		boolean writeAsSeed = seedOnly || archive.config.accessor.isSeedOnly();
		int type = writeAsSeed ? 1 : 0;
		
		return Util.concat(
				Util.serializeShort((short) 0), // version
				new byte[] { (byte) type },
				archive.config.accessor.seedRoot.getRaw(),
				keyIfDesired(!writeAsSeed, archive.config.accessor.passphraseRoot),
				keyIfDesired(!writeAsSeed, archive.config.writeRoot),
				archive.config.archiveId
				);
	}
	
	protected byte[] keyIfDesired(boolean desired, Key key) {
		if(!desired || key == null) return new byte[archive.crypto.symKeyLength()];
		return key.getRaw();
	}
	
	protected boolean isBlank(byte[] array) {
		for(byte b : array) {
			if(b != 0) return false;
		}
		
		return true;
	}
	
	protected void deserialize(ZKMaster master, ByteBuffer buf) throws IOException {
		int version = Util.unsignShort(buf.getShort());
		if(version != 0) throw new EINVALException("unsupported version");
		
		byte type = buf.get();
		if(type > 1) throw new EINVALException("unsupported record type");
		
		byte[] seedKeyRaw = new byte[master.crypto.symKeyLength()];
		buf.get(seedKeyRaw);
		
		byte[] passphraseKeyRaw = new byte[master.crypto.symKeyLength()];
		buf.get(passphraseKeyRaw);
		
		byte[] writeKeyRaw = new byte[master.crypto.symKeyLength()];
		buf.get(writeKeyRaw);
		
		archiveId = new byte[master.crypto.hashLength()];
		buf.get(archiveId);
		
		this.seedOnly = type != 0;
		Key seedKey = new Key(master.crypto, seedKeyRaw);
		Key passphraseKey = isBlank(passphraseKeyRaw) ? null : new Key(master.crypto, passphraseKeyRaw);
		
		writeKey = isBlank(writeKeyRaw) ? null : new Key(master.crypto, writeKeyRaw);
		accessor = master.makeAccessorForRoot(seedOnly ? seedKey : passphraseKey, seedOnly);
		ZKArchiveConfig config = ZKArchiveConfig.openExisting(accessor, archiveId, false, writeKey);
		if(config.haveConfigLocally()) {
			config.finishOpening();
			archive = config.getArchive();
		}
		
		locallyInstantiated = true;
	}
}
