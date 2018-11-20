package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor.ArchiveDiscovery;
import com.acrescrypto.zksync.utility.Util;

public class StoredAccess implements ArchiveDiscovery {
	public final static int ACCESS_LEVEL_NONE = 0;
	public final static int ACCESS_LEVEL_SEED = 1;
	public final static int ACCESS_LEVEL_READ = 2;
	public final static int ACCESS_LEVEL_READWRITE = 3;
	
	protected ArrayList<StoredAccessRecord> records = new ArrayList<StoredAccessRecord>();
	protected ZKMaster master;
	protected Key storageKey;
	
	public StoredAccess(ZKMaster master) {
		this.master = master;
		storageKey = master.localKey.derive(ArchiveAccessor.KEY_INDEX_STORED_ACCESS, new byte[0]);
	}
	
	public void storeArchiveAccess(ZKArchiveConfig config, int accessLevel) throws IOException {
		for(StoredAccessRecord record : records) {
			if(Arrays.equals(config.archiveId, record.archiveId)) {
				if(record.accessLevel == accessLevel) return;
				records.remove(record);
				break;
			}
		}
		
		records.add(new StoredAccessRecord(config, accessLevel));
		write();
	}
	
	public StoredAccessRecord recordForArchiveId(byte[] archiveId) {
		for(StoredAccessRecord record : records) {
			if(Arrays.equals(record.archiveId, archiveId)) return record;
		}
		
		return null;
	}
	
	public void deleteArchiveAccess(ZKArchiveConfig config) throws IOException {
		StoredAccessRecord killableRecord = null;
		for(StoredAccessRecord record : records) {
			if(Arrays.equals(record.getConfig().archiveId, config.archiveId)) {
				killableRecord = record;
				break;
			}
		}
		
		if(killableRecord != null) {
			records.remove(killableRecord);
			write();
		}
		
		master.removedArchiveConfig(config);
	}
	
	public void purge() throws IOException {
		for(StoredAccessRecord record : records) {
			master.removedArchiveConfig(record.getConfig());
		}
		
		records.clear();
		
		try {
			master.storage.unlink(path());
		} catch (ENOENTException exc) {
		}
	}
	
	public String path() {
		return "access";
	}
	
	public void read() throws IOException {
		try {
			MutableSecureFile file = new MutableSecureFile(master.storage, path(), storageKey);
			deserialize(file.read());
		} catch(ENOENTException exc) {
			records.clear();
			return;
		}
	}
	
	protected void write() throws IOException {
		MutableSecureFile file = new MutableSecureFile(master.storage, path(), storageKey);
		file.write(serialize(), 65536);
	}
	
	protected byte[] serialize() {
		/* TODO Someday: (review) ensure serialize() methods consistently return either plaintext and are wrapped by
		 * an encryption method as needed so we can rely on serialize() always returning plaintext.
		 */
		byte[][] recordBufs = new byte[records.size()][];
		int length = 0;
		
		for(int i = 0; i < records.size(); i++) {
			recordBufs[i] = records.get(i).serialize();
			length += recordBufs[i].length;
		}
		
		ByteBuffer plaintextBuf = ByteBuffer.allocate(length + 2*records.size());
		for(byte[] recordBuf : recordBufs) {
			assert(recordBuf.length <= Short.MAX_VALUE);
			plaintextBuf.putShort((short) recordBuf.length);
			plaintextBuf.put(recordBuf);
		}
		
		return plaintextBuf.array();
	}
	
	protected void deserialize(byte[] serialized) throws IOException {
		records = new ArrayList<StoredAccessRecord>();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		while(buf.hasRemaining()) {
			if(buf.remaining() < 2) throw new EINVALException("invalid stored access file");
			int length = Util.unsignShort(buf.getShort());
			if(buf.remaining() < length) throw new EINVALException("invalid stored access file");
			StoredAccessRecord record = new StoredAccessRecord(master, buf);
			records.add(record);
		}
	}

	@Override
	public void discoverArchives(ArchiveAccessor accessor) {
	}

	@Override
	public void stopDiscoveringArchives(ArchiveAccessor accessor) {
	}

	public void close() {
		for(StoredAccessRecord record : records) {
			record.close();
		}
	}
}
