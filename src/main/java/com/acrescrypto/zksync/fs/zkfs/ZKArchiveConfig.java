package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateSigningKey;
import com.acrescrypto.zksync.crypto.PublicSigningKey;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.utility.Util;

public class ZKArchiveConfig {
	
	public class InvalidArchiveConfigException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}
	
	public final static int CONFIG_MAGIC = 0x6CF2AA14;
	public final static int CONFIG_SECTION_ARCHIVE_INFO = 0x0001;
		
	protected byte[] archiveId; // derived from archive root; will later include public key

	protected Key archiveRoot; // randomly generated and stored encrypted in config file; derives most other keys
	protected Key writeRoot; // derives private key
	protected PrivateSigningKey privKey; // derived from the write key root
	protected PublicSigningKey pubKey; // matches privKey
	protected byte[] configFileIv; // rng
	protected BackedFS storage;
	protected FS localStorage;
	protected ArchiveAccessor accessor;
	protected int pageSize;
	protected String description;
	protected ZKArchive archive;
	protected PeerSwarm swarm;
	
	/** Read an existing archive. 
	 * @throws IOException */
	public ZKArchiveConfig(ArchiveAccessor accessor, byte[] archiveId) throws IOException {
		this.accessor = accessor;
		this.pageSize = -1;
		this.archiveId = archiveId;

		initStorage();
		
		read();
		this.archive = new ZKArchive(this);
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
		initStorage();
		this.archive = new ZKArchive(this);
		write();
	}
	
	protected void initStorage() throws IOException {
		this.swarm = new PeerSwarm(this);
		this.storage = new BackedFS(accessor.master.storageFsForArchiveId(archiveId), new SwarmFS(swarm));
		this.localStorage = accessor.master.localStorageFsForArchiveId(archiveId);
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
	
	protected int seedPortionPadSize() {
		return 256;
	}
	
	public void write() throws IOException {
		// TODO P2P: (refactor) consider futureproofing this. what if we actually want to use the reserved bytes someday?
		ByteBuffer writeBuf = ByteBuffer.allocate(pageSize+accessor.master.crypto.asymSignatureSize());
		byte[] seedPortion = serializeSeedPortion();
		byte[] securePortion = serializeSecurePortion();
		byte[] seedCiphertext = accessor.configFileSeedKey.encrypt(configFileIv, seedPortion, seedPortionPadSize());
		writeBuf.put(configFileIv);
		writeBuf.put(seedCiphertext);
		int padLen = writeBuf.remaining()-accessor.master.crypto.asymSignatureSize()-4-accessor.master.crypto.symTagLength();
		byte[] ciphertext = accessor.configFileKey.encrypt(configFileIv, securePortion, padLen);
		writeBuf.put(ciphertext);
		writeBuf.put(privKey.sign(writeBuf.array(), 0, writeBuf.position()));
		assertState(!writeBuf.hasRemaining());
		
		storage.write(Page.pathForTag(accessor.configFileTag), writeBuf.array());
	}
	
	public void read() throws IOException {
		try {
			ByteBuffer contents = ByteBuffer.wrap(storage.read(Page.pathForTag(accessor.configFileTag)));
			byte[] seedCiphertext = new byte[256 + accessor.master.crypto.symTagLength() + 4];
			assertState(contents.remaining() >= seedCiphertext.length);
			configFileIv = new byte[accessor.master.crypto.symIvLength()];
			contents.get(configFileIv);
			contents.get(seedCiphertext);
			deserializeSeedPortion(accessor.configFileSeedKey.decrypt(configFileIv, seedCiphertext));
			
			byte[] secureCiphertext = new byte[contents.remaining()-accessor.master.crypto.asymSignatureSize()];
			contents.get(secureCiphertext);
			if(!accessor.isSeedOnly()) {
				deserializeSecurePortion(accessor.configFileKey.decrypt(configFileIv, secureCiphertext));
				deriveKeypair();
			}
			
			int sigSize = accessor.master.crypto.asymSignatureSize();
			pubKey.assertValid(contents.array(), 0, contents.capacity()-sigSize, contents.array(), contents.capacity()-sigSize, sigSize);
		} catch(SecurityException exc) {
			throw new InvalidArchiveConfigException();
		}
	}
	
	public Key deriveKey(int root, int type, int index, byte[] tweak) {
		if(root != ArchiveAccessor.KEY_ROOT_ARCHIVE) return accessor.deriveKey(root, type, index, tweak);
		return archiveRoot.derive(((type & 0xFFFF) << 16) | (index & 0xFFFF), tweak);
	}
	
	public Key deriveKey(int root, int type, int index) {
		return deriveKey(root, type, index, new byte[0]);
	}
	
	public PrivateSigningKey getPrivKey() {
		return privKey;
	}
	
	public PublicSigningKey getPubKey() {
		return pubKey;
	}
	
	protected byte[] serializeSeedPortion() {
		ByteBuffer buf = ByteBuffer.allocate(pubKey.getBytes().length + accessor.master.crypto.hashLength());
		buf.put(pubKey.getBytes());
		buf.put(deriveArchiveFingerprint());
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
		buf.putInt(CONFIG_MAGIC);
		buf.putShort((short) CONFIG_SECTION_ARCHIVE_INFO);
		buf.putInt(archiveInfoSize);
		buf.putLong(pageSize);
		buf.put(archiveRoot.getRaw());
		buf.put(descString);
		
		assertState(!buf.hasRemaining());
		
		return buf.array();
	}
	
	protected void deserializeSeedPortion(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] pubKeyBytes = new byte[accessor.master.crypto.asymPublicSigningKeySize()];
		byte[] fingerprintBytes = new byte[accessor.master.crypto.hashLength()];
		assertState(buf.remaining() == pubKeyBytes.length + fingerprintBytes.length);
		buf.get(pubKeyBytes);
		buf.get(fingerprintBytes);
		
		try {
			this.pubKey = accessor.master.crypto.makePublicSigningKey(pubKeyBytes);
		} catch(IllegalArgumentException exc) {
			throw new InvalidArchiveConfigException();
		}
		assertState(Arrays.equals(archiveId, calculateArchiveId(fingerprintBytes)));
		assertState(Arrays.equals(configFileIv, calculateConfigFileIv()));
	}
	
	protected void deserializeSecurePortion(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		assertState(buf.getInt() == CONFIG_MAGIC);
		while(buf.hasRemaining()) {
			assertState(buf.remaining() >= 6); // 2-byte type + 4-byte length
			int type = Util.unsignShort(buf.getShort());
			int length = buf.getInt();
			
			assertState(length >= 0);
			assertState(buf.remaining() >= length);
			if(type != CONFIG_SECTION_ARCHIVE_INFO) {
				// only support one record type in this version...
				buf.position(buf.position() + length);
				continue;
			}
			
			assertState(length >= 8 + accessor.master.crypto.symKeyLength());
			
			long longPageSize = buf.getLong();
			assertState(longPageSize > 0 && longPageSize <= Integer.MAX_VALUE);
			this.pageSize = (int) longPageSize; // supporting long (2GB+) page sizes is not easy right now
			
			byte[] archiveRootRaw = new byte[accessor.master.crypto.symKeyLength()];
			buf.get(archiveRootRaw);
			this.archiveRoot = new Key(accessor.master.crypto, archiveRootRaw);
			byte[] descriptionRaw = new byte[length - 8 - accessor.master.crypto.symKeyLength()];
			buf.get(descriptionRaw);
			this.description = new String(descriptionRaw);
			break;
		}
	}
	
	protected void initArchiveSpecific() {
		archiveRoot = new Key(accessor.master.crypto, accessor.master.crypto.rng(accessor.master.crypto.symKeyLength()));
		deriveKeypair();
		archiveId = calculateArchiveId(deriveArchiveFingerprint());
		configFileIv = calculateConfigFileIv();
	}
	
	protected byte[] deriveArchiveFingerprint() {
		return accessor.seedRoot.authenticate(archiveRoot.getRaw());
	}
	
	protected byte[] calculateArchiveId(byte[] archiveFingerprint) {
		ByteBuffer keyMaterialBuf = ByteBuffer.allocate(archiveFingerprint.length + pubKey.getBytes().length);
		keyMaterialBuf.put(archiveFingerprint);
		keyMaterialBuf.put(pubKey.getBytes());
		assertState(!keyMaterialBuf.hasRemaining());
		byte[] id = accessor.seedRoot.authenticate(keyMaterialBuf.array());
		return id;
	}
	
	protected byte[] calculateConfigFileIv() {
		return accessor.master.crypto.expand(accessor.seedRoot.getRaw(), accessor.master.crypto.symIvLength(), "zksync".getBytes(), archiveId);
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new InvalidArchiveConfigException();
	}
	
	protected void deriveKeypair() {
		this.writeRoot = accessor.passphraseRoot; // TODO: allow some means of supplying a separate write passphrase
		privKey = accessor.master.crypto.makePrivateSigningKey(writeRoot.getRaw());
		if(pubKey != null) {
			assertState(Arrays.equals(pubKey.getBytes(), privKey.publicKey().getBytes()));
		}
		pubKey = privKey.publicKey();
	}

	public byte[] getArchiveId() {
		return archiveId;
	}

	public int getPageSize() {
		return pageSize;
	}
	
	public String getDescription() {
		return description;
	}

	public ArchiveAccessor getAccessor() {
		return accessor;
	}
	
	public PeerSwarm getSwarm() {
		return swarm;
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
	
	public FS getStorage() {
		return storage;
	}
	
	public FS getLocalStorage() {
		return localStorage;
	}
	
	public FS getCacheStorage() {
		return storage.getCacheFS();
	}

	public boolean validatePage(byte[] tag, byte[] allegedPage) {		
		Key authKey = deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_PAGE);
		int sigOffset = allegedPage.length - accessor.master.crypto.asymSignatureSize();
		if(!Arrays.equals(tag, authKey.authenticate(allegedPage))) return false;
		if(!pubKey.verify(allegedPage, 0, sigOffset, allegedPage, sigOffset, pubKey.getCrypto().asymSignatureSize())) return false;
		return true;
	}
}
