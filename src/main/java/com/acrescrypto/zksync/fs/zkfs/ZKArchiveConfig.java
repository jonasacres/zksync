package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.fs.compositefs.CompositeFS;

/** TODO P2P: Review key management.
 * Keeping separate cipher and auth keys from totally independent entropy sources is cumbersome, and probably not even a
 * serious security advantage. It was nice on paper, but it's at the point where I have to keep two separate seed keys,
 * which is not practical. Rework key hierarchy.
 *
 */

public class ZKArchiveConfig {
	
	public final static int CONFIG_MAGIC = 0x6CF2AA14;
	public final static int CONFIG_SECTION_ARCHIVE_INFO = 0x0001;
	
	public final static int KEY_ROOT_PASSPHRASE = 0;
	public final static int KEY_ROOT_ARCHIVE = 1;
	public final static int KEY_ROOT_SEED = 2;
	public final static int KEY_ROOT_LOCAL = 3;
	
	public final static int KEY_TYPE_CIPHER = 0;
	public final static int KEY_TYPE_AUTH = 1;
	public final static int KEY_TYPE_ROOT = 2;

	public final static int KEY_INDEX_ARCHIVE = 0;
	public final static int KEY_INDEX_PAGE = 1;
	public final static int KEY_INDEX_PAGE_MERKLE = 2;
	public final static int KEY_INDEX_REVISION = 3;
	public final static int KEY_INDEX_CONFIG_FILE = 4;
	public final static int KEY_INDEX_REVISION_TREE = 5;
	public final static int KEY_INDEX_SEED = 6;
	public final static int KEY_INDEX_SEED_REG = 7;
	public final static int KEY_INDEX_SEED_TEMPORAL = 8;
	public final static int KEY_INDEX_LOCAL = 9;
	
	protected byte[] archiveId; // derived from archive root; will later include public key (TODO)

	protected Key passphraseRoot; // derived from passphrase; used to generate seedRoot and configFileKey/Tag
	protected Key archiveRoot; // randomly generated and stored encrypted in config file; derives most other keys 
	protected Key seedRoot; // derived from passphrase; used to participate in DHT and peering, cannot decipher archives
	protected Key localRoot; // derived from locally-stored entropy combined with seedRoot; encrypts user preferences and any other data not shared with peers
	
	protected Key seedId; // derived from seed root; identifies archive family (all archives bearing the same passphrase)
	protected Key seedRegId; // derived from seed root; unique identifier for registering archive family
	
	protected Key configFileKey; // derived from passphrase root; used to encrypt configuration file
	protected byte[] configFileIv; // rng
	protected byte[] configFileTag; // derived from passphrase root; used to set location in filesystem of config file
	
	protected ZKMaster master;
	protected CompositeFS storage;
	
	// NOTE: in theory we support pageSize > MAX_INT, but in practice several operations cast to int and need a refactor
	// for this to work.
	protected long pageSize;
	protected String description;
	
	/** Read an existing archive. 
	 * @throws IOException */
	public ZKArchiveConfig(ZKMaster master, Key key, byte[] archiveId, CompositeFS storage, boolean isSeedKey) throws IOException {
		this.master = master;
		this.pageSize = -1;
		this.storage = storage;
		
		if(isSeedKey) {
			deriveFromPassphraseRoot(key);
		} else {
			deriveFromSeedRoot(key);
		}
		
		read();
	}
	
	/** Create a new archive. 
	 * @throws IOException */
	public ZKArchiveConfig(ZKMaster master, Key passphraseRoot, String description, int pageSize) throws IOException {
		// TODO: need to establish public key for signing here as well
		assert(pageSize > 0);
		this.master = master;
		this.pageSize = pageSize;
		this.description = description;
		deriveFromPassphraseRoot(passphraseRoot);
		initRoots();
		this.storage = new CompositeFS(master.storage.scopedFS("archives/" + Util.bytesToHex(archiveId)));
	}
	
	public boolean isSeedOnly() {
		return passphraseRoot != null;
	}
	
	public void isConfigAvailable() {
		storage.exists(Page.pathForTag(configFileTag));
	}
	
	public boolean isConfigLoaded() {
		return archiveRoot != null;
	}
	
	public byte[] temporalProof(int step, byte[] sharedSecret) {
		if(isSeedOnly()) return master.crypto.rng(master.crypto.symKeyLength()); // makes logic cleaner for protocol implementation
		assert(0 <= step && step <= Byte.MAX_VALUE);
		ByteBuffer timestamp = ByteBuffer.allocate(9);
		timestamp.putShort((byte) step);
		timestamp.putLong(System.currentTimeMillis());
		return passphraseRoot.derive(0x03, timestamp.array()).getRaw();
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
		pageSize = contents.getLong();
		
		int descLen = Util.unsignShort(contents.getShort());
		byte[] desc = new byte[descLen];
		contents.get(desc);
		description = new String(desc);
		
		byte[] archiveRootRaw = new byte[master.crypto.symKeyLength()];		
		contents.get(archiveRootRaw);
		archiveRoot = new Key(this.master.crypto, archiveRootRaw);
	}
	
	public byte[] getArchiveId() {
		return archiveId;
	}
	
	public long getPageSize() {
		return pageSize;
	}
	
	public void write() throws IOException {
		ByteBuffer writeBuf = ByteBuffer.allocate((int) pageSize);
		writeBuf.put(configFileIv);
		byte[] ciphertext = configFileKey.encrypt(configFileIv, serialize(), (int) pageSize - configFileIv.length);
		writeBuf.put(ciphertext);
		assertState(!writeBuf.hasRemaining());
		
		storage.write(Page.pathForTag(configFileTag), writeBuf.array());
	}
	
	public void read() throws IOException {
		byte[] ciphertext = storage.read(Page.pathForTag(configFileTag));
		byte[] serialized = configFileKey.decrypt(configFileIv, ciphertext);
		deserialize(serialized);
	}
	
	public Key deriveKey(int root, int type, int index, byte[] tweak) {
		Key[] keys = { passphraseRoot, archiveRoot, seedRoot, localRoot };
		if(type >= keys.length) throw new IllegalArgumentException();
		return keys[root].derive(((type & 0xFFFF) << 16) | (index & 0xFFFF), tweak);
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
			assertState(length >= 8 + master.crypto.symKeyLength());
			assertState(buf.remaining() >= length);
			if(type != CONFIG_SECTION_ARCHIVE_INFO) {
				// only support one record type in this version...
				buf.position(buf.position() + length);
				continue;
			}
			
			this.pageSize = buf.getLong();
			assertState(this.pageSize > 0 && this.pageSize < Integer.MAX_VALUE); // supporting really long pages is not easy right now
			
			byte[] archiveRootRaw = new byte[master.crypto.symKeyLength()];
			this.archiveRoot = new Key(master.crypto, archiveRootRaw);
			byte[] descriptionRaw = new byte[length - 8 + master.crypto.symKeyLength()];
			buf.get(descriptionRaw);
			this.description = new String(descriptionRaw);
			break;
		}
	}
	
	protected void initRoots() {
		archiveRoot = new Key(master.crypto, master.crypto.rng(master.crypto.symKeyLength()));
		configFileIv = master.crypto.rng(master.crypto.symIvLength());
		calculateArchiveId();
	}
	
	protected void deriveFromPassphraseRoot(Key passphraseRoot) {
		this.passphraseRoot = passphraseRoot;
		deriveFromSeedRoot(deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_ROOT, KEY_INDEX_SEED));		
		configFileKey = deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_CIPHER, KEY_INDEX_CONFIG_FILE, new byte[0]);
		configFileTag = deriveKey(KEY_ROOT_PASSPHRASE, KEY_TYPE_AUTH, KEY_INDEX_CONFIG_FILE, new byte[0]).getRaw();
	}
	
	protected void deriveFromSeedRoot(Key seedRoot) {
		this.seedRoot = seedRoot;
		seedId = deriveKey(KEY_ROOT_SEED, KEY_TYPE_AUTH, KEY_INDEX_SEED, new byte[0]);
		seedRegId = deriveKey(KEY_ROOT_SEED, KEY_TYPE_AUTH, KEY_INDEX_SEED_REG, new byte[0]);
		localRoot = deriveKey(KEY_ROOT_SEED, KEY_TYPE_ROOT, KEY_INDEX_LOCAL, master.localKey.getRaw());
	}
	
	protected Key keyFileTextKey(byte[] passphrase) {
		return new Key(master.crypto, master.crypto.deriveKeyFromPassphrase(passphrase, "zksync-salt".getBytes()));
	}
	
	protected Key temporalSeedId(int offset) {
		return temporalSeedDerivative(true, offset);
	}
	
	protected Key temporalSeedKey(int offset) {
		return temporalSeedDerivative(false, offset);
	}
	
	protected Key temporalSeedDerivative(boolean isAuth, int offset) {
		ByteBuffer timeTweak = ByteBuffer.allocate(8);
		timeTweak.putLong(timeSlice(offset));
		return deriveKey(KEY_ROOT_SEED, isAuth ? KEY_TYPE_AUTH : KEY_TYPE_CIPHER, KEY_INDEX_SEED_TEMPORAL, timeTweak.array());
	}
	
	protected long timeSlice(int offset) {
		return timeSliceInterval() * (System.currentTimeMillis()/timeSliceInterval() + offset);
	}
	
	protected int timeSliceInterval() {
		return 1000*60*60*3; // 3 hours
	}
	
	protected void calculateArchiveId() {
		ByteBuffer keyMaterialBuf = ByteBuffer.allocate(archiveRoot.getRaw().length);
		keyMaterialBuf.put(archiveRoot.getRaw());
		assertState(!keyMaterialBuf.hasRemaining());
		archiveId = passphraseRoot.authenticate(keyMaterialBuf.array());
		// TODO: needs to include signing key
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new RuntimeException();
	}
}
