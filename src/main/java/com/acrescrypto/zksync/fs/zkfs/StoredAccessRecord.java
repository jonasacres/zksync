package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.EINVALException;

public class StoredAccessRecord {
	protected ZKArchive archive;
	protected boolean seedOnly;
	
	public StoredAccessRecord(ZKArchive archive, boolean seedOnly) {
		this.seedOnly = seedOnly;
		this.archive = archive;
	}
	
	public StoredAccessRecord(ZKMaster master, ByteBuffer buf) throws IOException {
		deserialize(master, buf);
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	protected byte[] serialize() {
		boolean writeAsSeed = seedOnly || archive.config.accessor.isSeedOnly();
		int type = writeAsSeed ? ArchiveAccessor.KEY_INDEX_SEED : ArchiveAccessor.KEY_ROOT_PASSPHRASE;
		Key key = writeAsSeed ? archive.config.accessor.seedRoot : archive.config.accessor.passphraseRoot;
		
		ByteBuffer buf = ByteBuffer.allocate(2+1 + archive.master.crypto.symKeyLength() + archive.master.crypto.hashLength());
		buf.putShort((short) 0); // version
		buf.put((byte) type); // type
		buf.put(key.getRaw());
		buf.put(archive.config.archiveId);
		return buf.array();
	}
	
	protected void deserialize(ZKMaster master, ByteBuffer buf) throws IOException {
		int version = Util.unsignShort(buf.getShort());
		if(version != 0) throw new EINVALException("unsupported version");
		
		byte type = buf.get();
		if(type > 1) throw new EINVALException("unsupported record type");
		
		byte[] keyMaterial = new byte[master.crypto.symKeyLength()];
		buf.get(keyMaterial);
		
		byte[] archiveId = new byte[master.crypto.hashLength()];
		buf.get(archiveId);
		
		this.seedOnly = type != 0;
		Key key = new Key(master.crypto, keyMaterial);
		
		ArchiveAccessor accessor = new ArchiveAccessor(master, key, type);
		ZKArchiveConfig config = new ZKArchiveConfig(accessor, archiveId);
		archive = new ZKArchive(config);
	}
}
