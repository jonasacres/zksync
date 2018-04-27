package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.compositefs.CompositeFS;

public class StoredAccessRecord {
	protected ZKArchive archive;
	protected boolean seedOnly;
	
	public StoredAccessRecord(ZKArchive archive, boolean seedOnly) {
		this.seedOnly = seedOnly || archive.config.isSeedOnly();
		this.archive = archive;
	}
	
	public StoredAccessRecord(ZKMaster master, ByteBuffer buf) throws IOException {
		deserialize(master, buf);
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(2+1 + archive.master.crypto.symKeyLength() + archive.master.crypto.hashLength());
		buf.putShort((short) 0); // version
		buf.put((byte) (seedOnly ? 1 : 0)); // type
		buf.put((seedOnly ? archive.config.passphraseRoot : archive.config.seedRoot).getRaw());
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
		
		// TODO P2P: build this path in a method somewhere
		CompositeFS fs = new CompositeFS(master.storage.scopedFS("archives/" + Util.bytesToHex(archiveId)));
		ZKArchiveConfig config = new ZKArchiveConfig(master, key, archiveId, fs, seedOnly);
		archive = new ZKArchive(config);
	}
}
