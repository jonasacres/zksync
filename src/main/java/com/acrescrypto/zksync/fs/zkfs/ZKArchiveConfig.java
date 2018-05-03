package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.compositefs.CompositeFS;

public class ZKArchiveConfig {
	
	public final static int CONFIG_MAGIC = 0x6CF2AA14;
	public final static int CONFIG_SECTION_ARCHIVE_INFO = 0x0001;
	
	
	protected byte[] archiveId; // derived from archive root; will later include public key

	protected Key archiveRoot; // randomly generated and stored encrypted in config file; derives most other keys 
	protected byte[] configFileIv; // rng
	protected CompositeFS storage;
	protected ArchiveAccessor accessor;
	protected int pageSize;
	protected String description;
	
	/** Read an existing archive. 
	 * @throws IOException */
	public ZKArchiveConfig(ArchiveAccessor accessor, byte[] archiveId) throws IOException {
		this.accessor = accessor;
		this.pageSize = -1;
		this.storage = new CompositeFS(accessor.master.storageFsForArchiveId(archiveId));
		
		if(!accessor.isSeedOnly()) {
			read();
		}
		
		read();
	}
	
	/** Create a new archive. 
	 * @throws IOException */
	public ZKArchiveConfig(ArchiveAccessor accessor, String description, int pageSize) throws IOException {
		// TODO: need to establish public key for signing here as well
		assert(pageSize > 0);
		assert(!accessor.isSeedOnly());
		
		this.accessor = accessor;
		this.pageSize = pageSize;
		this.description = description;
		
		initArchiveSpecific();
		this.storage = new CompositeFS(accessor.master.storageFsForArchiveId(archiveId));
	}
	
	public void isConfigAvailable() {
		storage.exists(Page.pathForTag(accessor.configFileTag));
	}
	
	public boolean isConfigLoaded() {
		return archiveRoot != null;
	}
	
	public void parseFile(ByteBuffer contents) {
		assertState(contents.getLong() == CONFIG_MAGIC);
		while(contents.hasRemaining()) {
			int type = Util.unsignShort(contents.getShort());
			int length = Util.unsignShort(contents.getShort());
			int expectedIndex = contents.position() + length;
			
			assertState(contents.remaining() >= length);
			switch(type) {
			case CONFIG_SECTION_ARCHIVE_INFO:
				parseArchiveInfo(contents);
				break;
			default:
				assertState(false);
			}
			
			assertState(contents.position() == expectedIndex);
		}
	}
	
	public void parseArchiveInfo(ByteBuffer contents) {
		long longPageSize = contents.getLong();
		assertState(0 < longPageSize && longPageSize <= Integer.MAX_VALUE);
		pageSize = (int) longPageSize;
		
		int descLen = Util.unsignShort(contents.getShort());
		byte[] desc = new byte[descLen];
		contents.get(desc);
		description = new String(desc);
		
		byte[] archiveRootRaw = new byte[accessor.master.crypto.symKeyLength()];		
		contents.get(archiveRootRaw);
		archiveRoot = new Key(accessor.master.crypto, archiveRootRaw);
	}
	
	public byte[] getArchiveId() {
		return archiveId;
	}
	
	public int getPageSize() {
		return pageSize;
	}
	
	public void write() throws IOException {
		ByteBuffer writeBuf = ByteBuffer.allocate(pageSize);
		writeBuf.put(configFileIv);
		byte[] ciphertext = accessor.configFileKey.encrypt(configFileIv, serialize(), pageSize - configFileIv.length);
		writeBuf.put(ciphertext);
		assertState(!writeBuf.hasRemaining());
		
		storage.write(Page.pathForTag(accessor.configFileTag), writeBuf.array());
	}
	
	public void read() throws IOException {
		byte[] ciphertext = storage.read(Page.pathForTag(accessor.configFileTag));
		byte[] serialized = accessor.configFileKey.decrypt(configFileIv, ciphertext);
		deserialize(serialized);
	}
	
	public Key deriveKey(int root, int type, int index, byte[] tweak) {
		if(root != ArchiveAccessor.KEY_ROOT_PASSPHRASE) return accessor.deriveKey(root, type, index, tweak);
		return archiveRoot.derive(((type & 0xFFFF) << 16) | (index & 0xFFFF), tweak);
	}
	
	public Key deriveKey(int root, int type, int index) {
		return deriveKey(root, type, index, new byte[0]);
	}
	
	protected byte[] serialize() {
		byte[] descString = description.getBytes();
		int headerSize = 4; // magic
		int sectionHeaderSize = 2 + 4; // section_type + length
		int archiveInfoSize = 8 + archiveRoot.getRaw().length + descString.length; // pageSize + textRoot + authRoot + description
		
		assertState(descString.length <= Short.MAX_VALUE);
		
		ByteBuffer buf = ByteBuffer.allocate(headerSize+sectionHeaderSize+archiveInfoSize);
		buf.putLong(CONFIG_MAGIC);
		buf.putShort((short) CONFIG_SECTION_ARCHIVE_INFO);
		buf.putInt((short) archiveInfoSize);
		buf.putLong(pageSize);
		buf.put(archiveRoot.getRaw());
		buf.put(descString);
		
		assertState(!buf.hasRemaining());
		
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		assertState(buf.getLong() == CONFIG_MAGIC);
		while(buf.hasRemaining()) {
			assertState(buf.remaining() >= 6); // 2-byte type + 4-byte length
			int type = Util.unsignShort(buf.getShort());
			int length = buf.getInt();
			assertState(length >= 8 + accessor.master.crypto.symKeyLength());
			assertState(buf.remaining() >= length);
			if(type != CONFIG_SECTION_ARCHIVE_INFO) {
				// only support one record type in this version...
				buf.position(buf.position() + length);
				continue;
			}
			
			long longPageSize = buf.getLong();
			assertState(longPageSize > 0 && longPageSize <= Integer.MAX_VALUE);
			this.pageSize = (int) longPageSize; // supporting long (2GB+) page sizes is not easy right now
			
			byte[] archiveRootRaw = new byte[accessor.master.crypto.symKeyLength()];
			this.archiveRoot = new Key(accessor.master.crypto, archiveRootRaw);
			byte[] descriptionRaw = new byte[length - 8 + accessor.master.crypto.symKeyLength()];
			buf.get(descriptionRaw);
			this.description = new String(descriptionRaw);
			break;
		}
	}
	
	protected void initArchiveSpecific() {
		archiveRoot = new Key(accessor.master.crypto, accessor.master.crypto.rng(accessor.master.crypto.symKeyLength()));
		configFileIv = accessor.master.crypto.rng(accessor.master.crypto.symIvLength());
		calculateArchiveId();
	}
	
	protected void calculateArchiveId() {
		ByteBuffer keyMaterialBuf = ByteBuffer.allocate(archiveRoot.getRaw().length);
		keyMaterialBuf.put(archiveRoot.getRaw());
		assertState(!keyMaterialBuf.hasRemaining());
		archiveId = accessor.passphraseRoot.authenticate(keyMaterialBuf.array());
		// TODO: needs to include signing key
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new RuntimeException();
	}

	public ArchiveAccessor getAccessor() {
		return accessor;
	}
}
