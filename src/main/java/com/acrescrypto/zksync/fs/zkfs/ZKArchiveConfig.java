package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateKey;
import com.acrescrypto.zksync.crypto.PublicKey;
import com.acrescrypto.zksync.fs.compositefs.CompositeFS;
import com.acrescrypto.zksync.utility.Util;

public class ZKArchiveConfig {
	
	public final static int CONFIG_MAGIC = 0x6CF2AA14;
	public final static int CONFIG_SECTION_ARCHIVE_INFO = 0x0001;
	
	
	protected byte[] archiveId; // derived from archive root; will later include public key

	protected Key archiveRoot; // randomly generated and stored encrypted in config file; derives most other keys
	protected Key writeRoot; // derives private key
	protected PrivateKey privKey; // derived from the write key root
	protected PublicKey pubKey; // matches privKey
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
	}
	
	/** Create a new archive. 
	 * @throws IOException */
	public ZKArchiveConfig(ArchiveAccessor accessor, String description, int pageSize) throws IOException {
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
		byte[] seedPortion = serializeSeedPortion();
		byte[] seedCiphertext = accessor.configFileSeedKey.encrypt(configFileIv, seedPortion, 256);
		writeBuf.put(seedCiphertext);
		byte[] ciphertext = accessor.configFileKey.encrypt(configFileIv, serializeSecurePortion(), writeBuf.remaining());
		writeBuf.put(ciphertext);
		assertState(!writeBuf.hasRemaining());
		// TODO P2P: (implement) Add a signature
		
		storage.write(Page.pathForTag(accessor.configFileTag), writeBuf.array());
	}
	
	public void read() throws IOException {
		ByteBuffer contents = ByteBuffer.wrap(storage.read(Page.pathForTag(accessor.configFileTag)));
		byte[] seedCiphertext = new byte[256];
		contents.get(seedCiphertext);
		deserializeSeedPortion(accessor.configFileSeedKey.decrypt(configFileIv, seedCiphertext));
		
		byte[] secureCiphertext = new byte[contents.remaining()];
		contents.get(secureCiphertext);
		deserializeSecurePortion(accessor.configFileKey.decrypt(configFileIv, secureCiphertext));
	}
	
	public Key deriveKey(int root, int type, int index, byte[] tweak) {
		if(root != ArchiveAccessor.KEY_ROOT_ARCHIVE) return accessor.deriveKey(root, type, index, tweak);
		return archiveRoot.derive(((type & 0xFFFF) << 16) | (index & 0xFFFF), tweak);
	}
	
	public Key deriveKey(int root, int type, int index) {
		return deriveKey(root, type, index, new byte[0]);
	}
	
	public PrivateKey getPrivKey() {
		return privKey;
	}
	
	public PublicKey getPubKey() {
		return pubKey;
	}
	
	protected byte[] serializeSeedPortion() {
		ByteBuffer buf = ByteBuffer.allocate(pubKey.getBytes().length + accessor.master.crypto.hashLength());
		buf.put(pubKey.getBytes());
		buf.put(accessor.seedRoot.authenticate(archiveRoot.getRaw()));
		assertState(!buf.hasRemaining());
		return buf.array();
	}
	
	protected byte[] serializeSecurePortion() {
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
	
	protected void deserializeSeedPortion(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] pubKeyBytes = new byte[PublicKey.KEY_SIZE];
		byte[] fingerprintBytes = new byte[accessor.master.crypto.hashLength()];
		assertState(buf.remaining() == pubKeyBytes.length + fingerprintBytes.length);
		buf.get(pubKeyBytes);
		buf.get(fingerprintBytes);
		
		this.pubKey = new PublicKey(pubKeyBytes);
		assertState(Arrays.equals(archiveId, calculateArchiveId(fingerprintBytes)));
	}
	
	protected void deserializeSecurePortion(byte[] serialized) {
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
		deriveKeypair();
		archiveId = calculateArchiveId(deriveArchiveFingerprint());
	}
	
	protected byte[] deriveArchiveFingerprint() {
		return accessor.seedRoot.authenticate(archiveRoot.getRaw());
	}
	
	protected byte[] calculateArchiveId(byte[] archiveFingerprint) {
		ByteBuffer keyMaterialBuf = ByteBuffer.allocate(archiveFingerprint.length + pubKey.getBytes().length);
		keyMaterialBuf.put(archiveFingerprint);
		keyMaterialBuf.put(pubKey.getBytes());
		assertState(!keyMaterialBuf.hasRemaining());
		return accessor.seedRoot.authenticate(keyMaterialBuf.array());
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new RuntimeException();
	}
	
	protected void deriveKeypair() {
		this.writeRoot = accessor.passphraseRoot; // TODO P2P: (refactor) allow some means of supplying a separate write passphrase
		privKey = new PrivateKey(writeRoot.getRaw());
		pubKey = privKey.publicKey();
	}

	public ArchiveAccessor getAccessor() {
		return accessor;
	}
}
