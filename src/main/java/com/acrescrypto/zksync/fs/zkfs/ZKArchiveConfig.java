package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateSigningKey;
import com.acrescrypto.zksync.crypto.PublicSigningKey;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.utility.Util;

/** TODO Someday: (refactor) Refactor ZKArchiveConfig/ZKArchive/ArchiveAccessor into a more convenient/straightforward model.
 * 
 * I'm not thrilled with the way the ArchiveAccessor/ZKArchiveConfig/ZKArchive division has played out in
 * real life. The process of bootstrapping an archive is painful, confusing and delicate. My deeply-held suspicion
 * is that this will be a bottomless well of bugs.
 * 
 * Config should deal only with the config file. Archive should deal only with managing storage and retrieval of data.
 * And there should probably be some parent of config and archive that provides a coherent view of the archive, regardless
 * of whether we've obtained the config file yet.
 */

public class ZKArchiveConfig {
	public class InvalidArchiveConfigException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}
	
	public final static int CONFIG_MAGIC = 0x6CF2AA14;
	public final static int CONFIG_SECTION_ARCHIVE_INFO = 0x0001;
		
	protected byte[] archiveId; // derived from archive root; will later include public key
	protected byte[] archiveFingerprint;
	
	protected Key archiveRoot; // randomly generated and stored encrypted in config file; derives most other keys
	protected Key writeRoot; // derives private key
	protected PrivateSigningKey privKey; // derived from the write key root
	protected PublicSigningKey pubKey; // matches privKey
	protected byte[] configFileIv; // rng
	protected BackedFS storage;
	protected FS localStorage;
	protected ArchiveAccessor accessor;
	protected int pageSize, tagsPerChunk;
	protected String description;
	protected ZKArchive archive;
	protected PeerSwarm swarm;
	protected RevisionList revisionList;
	protected RevisionTree revisionTree;
	protected boolean advertising;
	
	public static byte[] decryptArchiveId(ArchiveAccessor accessor, byte[] iv, byte[] encryptedArchiveId) {
		Key key = accessor.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_AD_ARCHIVE_ID);
		if(iv.length != key.getCrypto().symBlockSize()) {
			ByteBuffer truncated = ByteBuffer.allocate(key.getCrypto().symBlockSize());
			truncated.put(iv, 0, Math.min(iv.length, truncated.capacity()));
			iv = truncated.array();
		}
		
		return key.decryptCBC(iv, encryptedArchiveId);
	}
	
	public static ZKArchiveConfig createDefault(ArchiveAccessor accessor) throws IOException {
		return create(accessor, "", ZKArchive.DEFAULT_PAGE_SIZE, accessor.passphraseRoot, accessor.passphraseRoot);
	}
	
	public static ZKArchiveConfig create(ArchiveAccessor accessor, String description, int pageSize) throws IOException {
		Key archiveRoot = new Key(accessor.master.crypto);
		return create(accessor, description, pageSize, archiveRoot, archiveRoot);
	}
	
	public static ZKArchiveConfig create(ArchiveAccessor accessor, String description, int pageSize, Key archiveRoot, Key writeRoot) throws IOException {
		return new ZKArchiveConfig(accessor, description, pageSize, archiveRoot, writeRoot);
	}
	
	public static ZKArchiveConfig openExisting(ArchiveAccessor accessor, byte[] archiveId) throws IOException {
		return openExisting(accessor, archiveId, true, Key.blank(accessor.master.crypto));
	}
	
	public static ZKArchiveConfig openExisting(ArchiveAccessor accessor, byte[] archiveId, boolean finish, Key writeRoot) throws IOException {
		return new ZKArchiveConfig(accessor, archiveId, finish, writeRoot);
	}
	
	/* Bootstrapping the archive is a mess of chicken-and-egg problems, especially where PeerSwarm is concerned.
	 * So, we have the ability to defer finishing opening (i.e. reading the config file itself) while we solve those problems.
	 * */
	protected ZKArchiveConfig(ArchiveAccessor accessor, byte[] archiveId, boolean finish, Key writeRoot) throws IOException {
		this.accessor = accessor;
		this.pageSize = -1;
		this.archiveId = archiveId;
		this.writeRoot = writeRoot;

		initStorage();
		this.accessor.discoveredArchiveConfig(this);
		if(finish) {
			try {
				finishOpening();
			} catch(InvalidArchiveConfigException exc) {
				close();
				throw exc;
			}
		}
		
		this.revisionList = new RevisionList(this);
		this.revisionTree = new RevisionTree(this);
	}
	
	/** Create a new archive. 
	 * @throws IOException */
	protected ZKArchiveConfig(ArchiveAccessor accessor, String description, int pageSize, Key archiveRoot, Key writeRoot) throws IOException {
		assert(pageSize > 0);
		assert(!accessor.isSeedOnly());
		
		this.accessor = accessor;
		this.pageSize = pageSize;
		this.description = description;
		
		initArchiveSpecific(archiveRoot, writeRoot);
		initStorage();
		this.revisionList = new RevisionList(this);
		this.revisionTree = new RevisionTree(this);
		this.archive = new ZKArchive(this);
		this.accessor.discoveredArchiveConfig(this);
		write();
	}
	
	public ZKArchiveConfig finishOpeningFromSwarm(long timeoutMs) throws IOException {
		advertise();
		if(haveConfigLocally()) return this;
		swarm.waitForPeers(timeoutMs);
		if(swarm.getConnections().size() > 0) {
			this.finishOpening();
		} else {
			throw new SearchFailedException(); // TODO: this is probably not a great exception for this
		}
		return this;
	}
	
	public ZKArchiveConfig finishOpening() throws IOException {
		if(archive != null) return this;
		
		read();
		this.archive = new ZKArchive(this);
		this.accessor.discoveredArchiveConfig(this);
		return this;
	}
	
	protected void initStorage() throws IOException {
		this.localStorage = accessor.master.localStorageFsForArchiveId(archiveId);
		this.swarm = new PeerSwarm(this);
		this.storage = new BackedFS(accessor.master.storageFsForArchiveId(archiveId), new SwarmFS(swarm));
	}
	
	protected int seedPortionPadSize() {
		return 256;
	}
	
	public byte[] tag() {
		return accessor.configFileTagKey.authenticate(archiveId);
	}
	
	public void write() throws IOException {
		ByteBuffer writeBuf = ByteBuffer.allocate(getSerializedPageSize());
		byte[] versionPortion = serializeVersionPortion();
		byte[] seedPortion = serializeSeedPortion();
		byte[] securePortion = serializeSecurePortion();
		byte[] seedCiphertext = accessor.configFileSeedKey.encrypt(configFileIv, seedPortion, seedPortionPadSize());
		writeBuf.put(versionPortion);
		writeBuf.put(configFileIv);
		writeBuf.put(seedCiphertext);
		int padLen = writeBuf.remaining()-accessor.master.crypto.asymSignatureSize()-4-accessor.master.crypto.symTagLength();
		byte[] ciphertext = accessor.configFileKey.encrypt(configFileIv, securePortion, padLen);
		writeBuf.put(ciphertext);
		writeBuf.put(privKey.sign(writeBuf.array(), 0, writeBuf.position()));
		assertState(!writeBuf.hasRemaining());
		
		storage.write(Page.pathForTag(tag()), writeBuf.array());
		if(archive != null) archive.addPageTag(tag());
	}
	
	public boolean haveConfigLocally() {
		return storage.getCacheFS().exists(Page.pathForTag(tag()));
	}
	
	public void read() throws IOException {
		try {
			ByteBuffer contents = ByteBuffer.wrap(storage.read(Page.pathForTag(tag())));
			
			byte[] versionHash = new byte[accessor.master.crypto.hashLength()];
			assertState(contents.remaining() >= versionHash.length);
			contents.get(versionHash);
			deserializeVersionPortion(versionHash);

			byte[] seedCiphertext = new byte[accessor.master.crypto.symPaddedCiphertextSize(seedPortionPadSize())];
			assertState(contents.remaining() >= seedCiphertext.length);
			configFileIv = new byte[accessor.master.crypto.symIvLength()];
			contents.get(configFileIv);
			contents.get(seedCiphertext);
			deserializeSeedPortion(accessor.configFileSeedKey.decrypt(configFileIv, seedCiphertext));
			
			byte[] secureCiphertext = new byte[contents.remaining()-accessor.master.crypto.asymSignatureSize()];
			contents.get(secureCiphertext);
			byte[] fingerprint = accessor.master.crypto.hash(secureCiphertext);
			assertState(Arrays.equals(archiveId, calculateArchiveId(fingerprint)));
			
			if(!accessor.isSeedOnly()) {
				deserializeSecurePortion(accessor.configFileKey.decrypt(configFileIv, secureCiphertext));
				deriveKeypair();
			}
			
			archiveFingerprint = fingerprint;
			int sigSize = accessor.master.crypto.asymSignatureSize();
			pubKey.assertValid(contents.array(), 0, contents.capacity()-sigSize, contents.array(), contents.capacity()-sigSize, sigSize);
		} catch(SecurityException exc) {
			throw new InvalidArchiveConfigException();
		}
	}
	
	public boolean verify(byte[] serialized) {
		try {
			ByteBuffer contents = ByteBuffer.wrap(serialized);
			
			byte[] versionHash = new byte[accessor.master.crypto.hashLength()];
			contents.get(versionHash);
			if(!Arrays.equals(serializeVersionPortion(), versionHash)) return false;
			
			configFileIv = new byte[accessor.master.crypto.symIvLength()];
			contents.get(configFileIv);

			byte[] seedCiphertext = new byte[getCrypto().symPaddedCiphertextSize(seedPortionPadSize())];
			contents.get(seedCiphertext);
			byte[] seedPortion = accessor.configFileSeedKey.decrypt(configFileIv, seedCiphertext);
			
			ByteBuffer seedPlaintext = ByteBuffer.wrap(seedPortion);
			byte[] pubKeyBytes = new byte[accessor.master.crypto.asymPublicSigningKeySize()];
			seedPlaintext.get(pubKeyBytes);
			if(!Arrays.equals(configFileIv, calculateConfigFileIv(pubKeyBytes))) return false;
			
			long pageSizeLong = seedPlaintext.getLong();
			if(pageSizeLong < 0 || pageSizeLong > Integer.MAX_VALUE);
			
			byte[] secureCiphertext = new byte[contents.remaining()-accessor.master.crypto.asymSignatureSize()];
			contents.get(secureCiphertext);
			byte[] fingerprint = accessor.master.crypto.hash(secureCiphertext);

			PublicSigningKey allegedPubKey = accessor.master.crypto.makePublicSigningKey(pubKeyBytes);
			if(!Arrays.equals(archiveId, calculateArchiveId(fingerprint, pubKeyBytes, pageSizeLong))) return false;
			
			int sigSize = accessor.master.crypto.asymSignatureSize();
			allegedPubKey.assertValid(contents.array(), 0, contents.capacity()-sigSize, contents.array(), contents.capacity()-sigSize, sigSize);
			
			return true;
		} catch(SecurityException|BufferUnderflowException|IllegalArgumentException exc) {
			return false;
		}
	}
	
	public void close() {
		swarm.close();
		stopAdvertising();
	}
	
	public boolean isClosed() {
		return swarm.isClosed();
	}
	
	public Key deriveKey(int root, int type, int index, byte[] tweak) {
		int modifier = ((type & 0xFFFF) << 16) | (index & 0xFFFF);
		if(root == ArchiveAccessor.KEY_ROOT_ARCHIVE) { 
			return archiveRoot.derive(modifier, tweak);
		} else if(root == ArchiveAccessor.KEY_ROOT_WRITE) {
			return writeRoot.derive(modifier, tweak);
		} else {
			return accessor.deriveKey(root, type, index, tweak);
		}
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
	
	public void setPubKey(PublicSigningKey pubKey) {
		this.pubKey = pubKey;
	}
	
	public void setArchiveFingerprint(byte[] archiveFingerprint) {
		this.archiveFingerprint = archiveFingerprint;
	}
	
	public byte[] getArchiveFingerprint() {
		return archiveFingerprint;
	}
	
	public RevisionList getRevisionList() {
		return revisionList;
	}
	
	public RevisionTree getRevisionTree() {
		return revisionTree;
	}
	
	public boolean canReceive() {
		return pageSize > 0 && pubKey != null && archiveFingerprint != null;
	}
	
	public boolean hasKey() {
		return archiveRoot != null;
	}
	
	protected byte[] serializeVersionPortion() {
		byte[] effectiveArchiveId = new byte[accessor.master.crypto.hashLength()];
		if(archiveId != null) effectiveArchiveId = archiveId;
		
		ByteBuffer versionInput = ByteBuffer.allocate(4+effectiveArchiveId.length);
		versionInput.putInt(0);
		versionInput.put(effectiveArchiveId);
		return accessor.configFileTagKey.authenticate(versionInput.array()); 
	}
	
	protected byte[] serializeSeedPortion() {
		ByteBuffer buf = ByteBuffer.allocate(pubKey.getBytes().length + 8);
		buf.put(pubKey.getBytes());
		buf.putLong(pageSize);
		assertState(!buf.hasRemaining());
		return buf.array();
	}
	
	protected byte[] serializeSecurePortion() {
		byte[] descString = description.getBytes();
		int headerSize = 4; // magic
		int sectionHeaderSize = 2 + 4; // section_type + length
		int archiveInfoSize = archiveRoot.getRaw().length + descString.length; // textRoot + authRoot + description
		
		assertState(descString.length <= Short.MAX_VALUE);
		
		ByteBuffer buf = ByteBuffer.allocate(headerSize+sectionHeaderSize+archiveInfoSize);
		buf.putInt(CONFIG_MAGIC);
		buf.putShort((short) CONFIG_SECTION_ARCHIVE_INFO);
		buf.putInt(archiveInfoSize);
		buf.put(archiveRoot.getRaw());
		buf.put(descString);
		
		assertState(!buf.hasRemaining());
		
		return buf.array();
	}
	
	protected void deserializeVersionPortion(byte[] serialized) {
		assertState(Arrays.equals(serializeVersionPortion(), serialized));
	}
	
	protected void deserializeSeedPortion(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] pubKeyBytes = new byte[accessor.master.crypto.asymPublicSigningKeySize()];
		assertState(buf.remaining() == pubKeyBytes.length + 8);
		buf.get(pubKeyBytes);
		
		long pageSizeTemp = buf.getLong();
		assertState(0 < pageSizeTemp && pageSizeTemp <= Integer.MAX_VALUE);
		pageSize = (int) pageSizeTemp; // supporting long (2GB+) page sizes is not easy right now
		
		try {
			this.pubKey = accessor.master.crypto.makePublicSigningKey(pubKeyBytes);
		} catch(IllegalArgumentException exc) {
			throw new InvalidArchiveConfigException();
		}

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
			
			assertState(length >= accessor.master.crypto.symKeyLength());
			
			byte[] archiveRootRaw = new byte[accessor.master.crypto.symKeyLength()];
			buf.get(archiveRootRaw);
			this.archiveRoot = new Key(accessor.master.crypto, archiveRootRaw);
			byte[] descriptionRaw = new byte[length - accessor.master.crypto.symKeyLength()];
			buf.get(descriptionRaw);
			this.description = new String(descriptionRaw);
			break;
		}
	}
	
	protected void initArchiveSpecific(Key archiveRoot, Key writeRoot) {
		this.archiveRoot = archiveRoot;
		this.writeRoot = writeRoot;
		deriveKeypair();
		configFileIv = calculateConfigFileIv();
		archiveFingerprint = deriveArchiveFingerprint();
		archiveId = calculateArchiveId(archiveFingerprint);
	}
	
	protected byte[] deriveArchiveFingerprint() {
		byte[] securePortion = serializeSecurePortion();
		int remaining = getSerializedPageSize() - serializeVersionPortion().length - accessor.master.crypto.symIvLength() - accessor.master.crypto.symPaddedCiphertextSize(seedPortionPadSize());
		int padLen = remaining-accessor.master.crypto.asymSignatureSize()-4-accessor.master.crypto.symTagLength();
		byte[] ciphertext = accessor.configFileKey.encrypt(configFileIv, securePortion, padLen);
		return accessor.master.crypto.hash(ciphertext);
	}
	
	public byte[] calculateArchiveId(byte[] archiveFingerprint) {
		return calculateArchiveId(archiveFingerprint, pubKey.getBytes(), pageSize);
	}
	
	public byte[] calculateArchiveId(byte[] archiveFingerprint, byte[] pubKeyRaw, long pageSizeLong) {
		ByteBuffer keyMaterialBuf = ByteBuffer.allocate(archiveFingerprint.length + pubKeyRaw.length + 8);
		keyMaterialBuf.put(archiveFingerprint);
		keyMaterialBuf.put(pubKeyRaw);
		keyMaterialBuf.putLong(pageSizeLong);
		assertState(!keyMaterialBuf.hasRemaining());
		byte[] id = accessor.seedRoot.authenticate(keyMaterialBuf.array());
		return id;
	}
	
	protected byte[] calculateConfigFileIv() {
		return calculateConfigFileIv(pubKey.getBytes());
	}
	
	protected byte[] calculateConfigFileIv(byte[] pubKeyBytes) {
		return accessor.master.crypto.expand(accessor.seedRoot.getRaw(), accessor.master.crypto.symIvLength(), "zksync".getBytes(), pubKeyBytes);
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new InvalidArchiveConfigException();
	}
	
	protected void deriveKeypair() {
		if(writeRoot == null) return;
		
		if(writeRoot.isBlank()) {
			writeRoot = archiveRoot;
		}
		
		privKey = accessor.master.crypto.makePrivateSigningKey(writeRoot.getRaw());
		if(pubKey != null) {
			assertState(Arrays.equals(pubKey.getBytes(), privKey.publicKey().getBytes()));
		}
		pubKey = privKey.publicKey();
	}

	public byte[] getArchiveId() {
		return archiveId;
	}
	
	public byte[] getEncryptedArchiveId(byte[] iv) {
		Key key = deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_AD_ARCHIVE_ID);

		if(iv.length != key.getCrypto().symBlockSize()) {
			ByteBuffer truncated = ByteBuffer.allocate(key.getCrypto().symBlockSize());
			truncated.put(iv, 0, Math.min(iv.length, truncated.capacity()));
			iv = truncated.array();
		}
		
		return key.encryptCBC(iv, archiveId);
	}

	public int getPageSize() {
		return pageSize;
	}
	
	public int getTagsPerChunk() {
		if(tagsPerChunk > 0) return tagsPerChunk;
		tagsPerChunk = pageSize/archive.crypto.hashLength();
		return tagsPerChunk;
	}
	
	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}
	
	public int getSerializedPageSize() {
		return SignedSecureFile.fileSize(accessor.master.crypto, pageSize);
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
	
	public CryptoSupport getCrypto() {
		return accessor.master.getCrypto();
	}
	
	public int refTagSize() {
		return RefTag.REFTAG_EXTRA_DATA_SIZE + getCrypto().hashLength();
	}
	
	@Deprecated // Use for testing only!!
	public void setStorage(BackedFS storage) {
		this.storage = storage;
	}

	public boolean validatePage(byte[] tag, byte[] allegedPage) {
		if(Arrays.equals(tag, tag())) {
			return verify(allegedPage);
		}
		
		Key authKey = deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_PAGE);
		int sigOffset = allegedPage.length - accessor.master.crypto.asymSignatureSize();
		if(!Arrays.equals(tag, authKey.authenticate(allegedPage))) return false;
		if(!pubKey.verify(allegedPage, 0, sigOffset, allegedPage, sigOffset, pubKey.getCrypto().asymSignatureSize())) return false;
		return true;
	}

	public ZKMaster getMaster() {
		return accessor.getMaster();
	}
	
	public boolean equals(Object other) {
		if(!(other instanceof ZKArchiveConfig)) {
			return false;
		}
		
		if(Arrays.equals(archiveId, ((ZKArchiveConfig) other).archiveId)) {
			return accessor.isSeedOnly() == ((ZKArchiveConfig) other).accessor.isSeedOnly();
		}
		
		return false;
	}
	
	public ThreadGroup getThreadGroup() {
		return accessor.getThreadGroup();
	}

	public boolean isReadOnly() {
		return writeRoot == null;
	}
	
	public boolean usesWriteKey() {
		// TODO API: (coverage) coverage...
		if(archiveRoot == null) return true;
		PrivateSigningKey key = accessor.master.crypto.makePrivateSigningKey(archiveRoot.getRaw());
		return !Util.safeEquals(key.publicKey().getBytes(), pubKey.getBytes());
	}
	
	public void setWriteRoot(Key writeRoot) {
		this.writeRoot = writeRoot;
	}
	
	public void clearWriteRoot() {
		this.writeRoot = null;
	}
	
	public void advertise() {
		if(isAdvertising()) return;
		advertising = true;
		accessor.discoverOnDHT();
		accessor.master.getTCPListener().advertise(swarm);
	}
	
	public void stopAdvertising() {
		if(!isAdvertising()) return;
		advertising = false;
		accessor.master.getTCPListener().stopAdvertising(swarm);
		getMaster().getDHTDiscovery().stopDiscoveringArchives(accessor);
	}

	public boolean isAdvertising() {
		return advertising;
	}
	
	@Deprecated
	public void setSwarm(PeerSwarm swarm) {
		// test use only
		swarm.close();
		this.swarm = swarm;
	}
}
