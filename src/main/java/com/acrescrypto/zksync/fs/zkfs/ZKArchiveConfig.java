package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PrivateSigningKey;
import com.acrescrypto.zksync.crypto.PublicSigningKey;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidPageException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.swarmfs.SwarmFS;
import com.acrescrypto.zksync.fs.zkfs.config.SectionedBuffer;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.utility.Util;

/** TODO Someday: (refactor) Refactor ZKArchiveConfig/ZKArchive/ArchiveAccessor into a more convenient/straightforward model.
 * 
 * I'm not thrilled with the way the ArchiveAccessor/ZKArchiveConfig/ZKArchive division has played out in
 * real life. The process of bootstrapping an archive is painful, confusing and delicate. My deeply-held suspicion
 * is that this will be a bottomless well of bugs.
 * 
 * What's more, there are still too many responsibilities per-class. This needs to be split out even
 * further. And the nomenclature has evolved. What is called a "filesystem" in public parlance is an
 * "archive" in the code; what is a "filesystem" in the code is something else yet to be determined in
 * public.
 * 
 * Config should deal only with the config file. Archive should deal only with managing storage and retrieval of data.
 * And there should probably be some parent of config and archive that provides a coherent view of the archive, regardless
 * of whether we've obtained the config file yet.
 */

public class ZKArchiveConfig {
	public class InvalidArchiveConfigException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}
	
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
	protected Logger logger = LoggerFactory.getLogger(ZKArchiveConfig.class);
	
	public static byte[] decryptArchiveId(ArchiveAccessor accessor, byte[] iv, byte[] encryptedArchiveId) {
		Key key = accessor.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, "easysafe-dht-ad-fsid");
		if(iv.length != key.getCrypto().symIvLength()) {
			ByteBuffer truncated = ByteBuffer.allocate(key.getCrypto().symIvLength());
			truncated.put(iv, 0, Math.min(iv.length, truncated.capacity()));
			iv = truncated.array();
		}
		
		return key.decryptUnauthenticated(iv, encryptedArchiveId);
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
	
	protected ZKArchiveConfig() {}
	
	/* Bootstrapping the archive is a mess of chicken-and-egg problems, especially where PeerSwarm is concerned.
	 * So, we have the ability to defer finishing opening (i.e. reading the config file itself) while we solve those problems.
	 * */
	protected ZKArchiveConfig(ArchiveAccessor accessor, byte[] archiveId, boolean finish, Key writeRoot) throws IOException {
		this.accessor = accessor;
		this.pageSize = -1;
		this.archiveId = archiveId;
		this.writeRoot = writeRoot;

		initStorage();
		decodeId();
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
		write();
		this.revisionList = new RevisionList(this);
		this.revisionTree = new RevisionTree(this);
		this.archive = new ZKArchive(this);
		this.accessor.discoveredArchiveConfig(this);
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
		
		try {
			read();
		} catch(SecurityException exc) {
			throw new InvalidArchiveConfigException();
		}
		this.archive = new ZKArchive(this);
		return this;
	}
	
	protected void initStorage() throws IOException {
		this.localStorage = accessor.master.localStorageFsForArchiveId(archiveId);
		this.swarm = new PeerSwarm(this);
		this.storage = new BackedFS(accessor.master.storageFsForArchiveId(archiveId), new SwarmFS(swarm));
	}
	
	protected void decodeId() {
		assertState(archiveId.length == getCrypto().hashLength());
		int prefixLen = getCrypto().hashLength() - getCrypto().asymPublicSigningKeySize() - 4;
		byte[] prefix = new byte[prefixLen], suffix = new byte[archiveId.length - prefix.length];
		System.arraycopy(archiveId, 0, prefix, 0, prefix.length);
		System.arraycopy(archiveId, prefix.length, suffix, 0, suffix.length);
		Key suffixKey = accessor.getSeedRoot().derive("easysafe-config-fsid-suffix-key", prefix);
		
		ByteBuffer pt = ByteBuffer.wrap(suffixKey.decryptUnauthenticated(
				getCrypto().symNonce(0),
				suffix));
		byte[] pubKeyBytes = new byte[getCrypto().asymPublicSigningKeySize()];
		pt.get(pubKeyBytes);
		
		pageSize = pt.getInt();
		pubKey = getCrypto().makePublicSigningKey(pubKeyBytes);
	}
	
	public byte[] tag() {
		return accessor.seedRoot.authenticate(archiveId);
	}
	
	public boolean haveConfigLocally() {
		return storage.getCacheFS().exists(Page.pathForTag(tag()));
	}
	
	public void write() throws IOException {
		// TODO EasySafe: (implement) config write
		byte[] seedSection = serializeSeedSection();
		byte[] secureSection = serializeSecureSection();
		
		byte[] saltInput = Util.concat("easysafe-version:0".getBytes(),
				Util.serializeInt(seedSection.length),
				seedSection,
				Util.serializeInt(secureSection.length),
				secureSection);
		
		byte[] salt = accessor.getSeedRoot().authenticate(saltInput);

		byte[] versionSection = serializeVersionSection(salt);

		Key configSeedPreambleKey = accessor.seedRoot.derive("easysafe-config-seed-preamble-key",
				Util.concat(salt, versionSection));		
		byte[] seedPreamble = configSeedPreambleKey.encrypt(
				getCrypto().symNonce(0),
				Util.serializeInt(seedSection.length + getCrypto().symTagLength()),
				-1);
		
		Key configSeedTextKey = accessor.seedRoot.derive("easysafe-config-seed-ciphertext-key",
				Util.concat(salt, versionSection, seedPreamble));
		byte[] seedCiphertext = configSeedTextKey.encrypt(
				getCrypto().symNonce(0),
				seedSection,
				-1);
		
		Key configSecurePreambleKey = accessor.passphraseRoot.derive("easysafe-config-secure-preamble-key",
				Util.concat(salt, versionSection, seedPreamble, seedCiphertext));
		byte[] securePreamble = configSecurePreambleKey.encrypt(
				getCrypto().symNonce(0),
				Util.serializeInt(secureSection.length + getCrypto().symTagLength()),
				-1);
		
		
		Key configSecureTextKey = accessor.passphraseRoot.derive("easysafe-config-secure-ciphertext-key",
				Util.concat(salt, versionSection, seedPreamble, seedCiphertext, securePreamble));
		byte[] secureCiphertext = configSecureTextKey.encrypt(
				getCrypto().symNonce(0),
				secureSection,
				-1);
		
		// need the +4 because we're matching encrypted page size, and pages get 32 bit length field
		int padLen = getSerializedPageSize() - getCrypto().asymSignatureSize()
				- salt.length
				- versionSection.length
				- seedPreamble.length
				- seedCiphertext.length
				- securePreamble.length
				- secureCiphertext.length;
		Key paddingKey = archiveRoot.derive("easysafe-config-padding-key",
				Util.concat(salt,
						versionSection,
						seedPreamble,
						seedCiphertext,
						securePreamble,
						secureCiphertext));
		byte[] padding = paddingKey.encryptUnauthenticated(getCrypto().symNonce(0), new byte[padLen]);
		
		byte[] unsignedFile = Util.concat(salt,
				versionSection,
				seedPreamble,
				seedCiphertext,
				securePreamble,
				secureCiphertext,
				padding);
		byte[] signature = privKey.sign(unsignedFile);
		byte[] configFile = Util.concat(unsignedFile, signature);
		
		int prefixLen = getCrypto().hashLength() - getCrypto().asymPublicSigningKeySize() - 4;
		byte[] prefix = getCrypto().expand(configFile,
				prefixLen, 
				accessor.getSeedRoot().getRaw(),
				"easysafe-config-fsid-prefix".getBytes());
		
		ByteBuffer suffixData = ByteBuffer.allocate(getCrypto().hashLength() - prefixLen);
		suffixData.put(pubKey.getBytes());
		suffixData.putInt(pageSize);
		
		Key suffixKey = accessor.getSeedRoot().derive("easysafe-config-fsid-suffix-key", prefix);
		byte[] suffix = suffixKey.encryptUnauthenticated(getCrypto().symNonce(0), suffixData.array());
		archiveId = Util.concat(prefix, suffix);
		
		if(localStorage == null) {
			initStorage();
		}
		
		storage.write(Page.pathForTag(tag()), configFile);
	}
	
	public void read() throws IOException {
		// TODO EasySafe: (implement) config read
		waitForPageReady(tag());
		try(File configFile = storage.open(Page.pathForTag(tag()), File.O_RDONLY)) {
			// TODO EasySafe: (refactor) read the file exactly once
			byte[] prefix = calculatePrefix(configFile.read());
			assertState(Util.safeEquals(prefix, archiveId, prefix.length));
			configFile.rewind();

			byte[] salt = configFile.read(getCrypto().hashLength());
			byte[] versionSection = configFile.read(getCrypto().hashLength());
			deserializeVersionSection(salt, versionSection);
			
			byte[] seedPreambleCt = configFile.read(4 + getCrypto().symTagLength());
			Key configSeedPreambleKey = accessor.seedRoot.derive("easysafe-config-seed-preamble-key",
					Util.concat(salt, versionSection));
			byte[] seedPreamble = configSeedPreambleKey.decryptUnpadded(
					getCrypto().symNonce(0),
					seedPreambleCt);
			int seedLen = ByteBuffer.wrap(seedPreamble).getInt();
			
			byte[] seedSectionCt = configFile.read(seedLen);
			Key configSeedTextKey = accessor.seedRoot.derive("easysafe-config-seed-ciphertext-key",
					Util.concat(salt, versionSection, seedPreambleCt));
			byte[] seedSection = configSeedTextKey.decryptUnpadded(
					getCrypto().symNonce(0),
					seedSectionCt);

			deserializeSeedSection(seedSection);

			if(accessor.passphraseRoot != null) {
				byte[] securePreambleCt = configFile.read(4 + getCrypto().symTagLength());
				Key configSecurePreambleKey = accessor.passphraseRoot.derive("easysafe-config-secure-preamble-key",
						Util.concat(salt, versionSection, seedPreambleCt, seedSectionCt));
				byte[] securePreamble = configSecurePreambleKey.decryptUnpadded(
						getCrypto().symNonce(0),
						securePreambleCt);
				int secureLen = ByteBuffer.wrap(securePreamble).getInt();
				
				byte[] secureSectionCt = configFile.read(secureLen);
				Key configSecureTextKey = accessor.passphraseRoot.derive("easysafe-config-secure-ciphertext-key",
						Util.concat(salt, versionSection, seedPreambleCt, seedSectionCt, securePreambleCt));
				byte[] secureSection = configSecureTextKey.decryptUnpadded(
						getCrypto().symNonce(0),
						secureSectionCt);

				deserializeSecureSection(secureSection);
				deriveKeypair();
			}
		}
	}
	
	protected byte[] serializeVersionSection(byte[] salt) {
		return accessor.seedRoot.authenticate(Util.concat(salt, ("easysafe-version:0").getBytes()));
	}
	
	protected byte[] serializeSeedSection() {
		return new SectionedBuffer()
			.addRecord("page-size", pageSize)
			.addRecord("public-key", pubKey.getBytes())
			.serialize();
	}
	
	protected byte[] serializeSecureSection() {
		return new SectionedBuffer()
			.addRecord("archive-root", archiveRoot.getRaw())
			.addRecord("description", description)
			.serialize();
	}
	
	protected int deserializeVersionSection(byte[] salt, byte[] versionSection) {
		int maxSupported = 0;
		for(int i = 0; i <= maxSupported; i++) {
			byte[] candidate = accessor.seedRoot.authenticate(Util.concat(salt, ("easysafe-version:" + i).getBytes()));
			if(Arrays.equals(candidate, versionSection)) {
				return i;
			}
		}
		
		assertState(false);
		return -1; // unreachable
	}
	
	protected void deserializeSeedSection(byte[] seedSection) {
		SectionedBuffer sect = new SectionedBuffer(seedSection);
		byte[] pageSizeBytes = sect.contentForKey("page-size",
				Util.serializeInt(ZKArchive.DEFAULT_PAGE_SIZE));
		byte[] pubKeyBytes = sect.contentForKey("public-key");
		
		assertState(pubKeyBytes != null);
		assertState(Arrays.equals(pubKeyBytes, pubKey.getBytes()));
		pageSize = ByteBuffer.wrap(pageSizeBytes).getInt();
	}
	
	protected void deserializeSecureSection(byte[] secureSection) {
		SectionedBuffer sect = new SectionedBuffer(secureSection);
		byte[] archiveRootBytes = sect.contentForKey("archive-root", accessor.passphraseRoot.getRaw());
		byte[] descriptionBytes = sect.contentForKey("description", new byte[0]);
		
		archiveRoot = new Key(getCrypto(), archiveRootBytes);
		description = new String(descriptionBytes);
		if(description.indexOf('\0') >= 0) {
			description = description.substring(0, description.indexOf('\0'));
		}
	}
	
	protected byte[] calculatePrefix(byte[] serialized) {
		return getCrypto().expand(serialized,
				28, 
				accessor.getSeedRoot().getRaw(),
				"easysafe-config-fsid-prefix".getBytes());
	}
	
	public boolean verify(byte[] serialized) {
		if(serialized.length != getSerializedPageSize()) {
			return false;
		}
		
		byte[] observedPrefix = calculatePrefix(serialized);
		if(!Util.safeEquals(observedPrefix, archiveId, observedPrefix.length)) {
			return false;
		}
		
		if(!pubKey.verify(serialized,
				0,
				serialized.length - getCrypto().asymSignatureSize(),
				serialized,
				serialized.length - getCrypto().asymSignatureSize(),
				getCrypto().asymSignatureSize())) {
			return false;
		}
		
		return true;
	}
	
	public Key deriveKey(int root, String id, byte[] tweak) {
		if(root == ArchiveAccessor.KEY_ROOT_ARCHIVE) { 
			return archiveRoot.derive(id, tweak);
		} else if(root == ArchiveAccessor.KEY_ROOT_WRITE) {
			return writeRoot.derive(id, tweak);
		} else {
			return accessor.deriveKey(root, id, tweak);
		}
	}
	
	public Key deriveKey(int root, String id) {
		return deriveKey(root, id, new byte[0]);
	}
	
	protected void initArchiveSpecific(Key archiveRoot, Key writeRoot) {
		this.archiveRoot = archiveRoot;
		this.writeRoot = writeRoot;
		deriveKeypair();
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

	public boolean validatePage(byte[] tag, byte[] allegedPage) {
		if(Arrays.equals(tag, tag())) {
			return verify(allegedPage);
		}
		
		Key authKey = deriveKey(ArchiveAccessor.KEY_ROOT_SEED, "easysafe-page-auth-key");
		int sigOffset = allegedPage.length - accessor.master.crypto.asymSignatureSize();
		if(!Arrays.equals(tag, authKey.authenticate(allegedPage))) return false;
		if(!pubKey.verify(allegedPage, 0, sigOffset, allegedPage, sigOffset, pubKey.getCrypto().asymSignatureSize())) return false;
		return true;
	}

	public ZKMaster getMaster() {
		return accessor.getMaster();
	}
	
	public void advertise() {
		if(isAdvertising()) return;
		advertising = true;
		accessor.discoverOnDHT();
		accessor.master.getTCPListener().advertise(swarm);
		accessor.master.getDHTDiscovery().forceUpdate(accessor);
	}
	
	public void stopAdvertising() {
		if(!isAdvertising()) return;
		advertising = false;
		accessor.master.getTCPListener().stopAdvertising(swarm);
		getMaster().getDHTDiscovery().stopDiscoveringArchives(accessor);
	}
	
	public boolean hasKey() {
		return archiveRoot != null;
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
	
	public boolean isAdvertising() {
		return advertising;
	}
	
	public ThreadGroup getThreadGroup() {
		return accessor.getThreadGroup();
	}

	public byte[] getArchiveId() {
		return archiveId;
	}
	
	public byte[] getEncryptedArchiveId(byte[] iv) {
		Key key = deriveKey(ArchiveAccessor.KEY_ROOT_SEED, "easysafe-dht-ad-fsid");

		if(iv.length != key.getCrypto().symIvLength()) {
			ByteBuffer truncated = ByteBuffer.allocate(key.getCrypto().symIvLength());
			truncated.put(iv, 0, Math.min(iv.length, truncated.capacity()));
			iv = truncated.array();
		}
		
		return key.encryptUnauthenticated(iv, archiveId);
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

	public void setWriteRoot(Key writeRoot) {
		this.writeRoot = writeRoot;
	}
	
	public void clearWriteRoot() {
		this.writeRoot = null;
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
	
	public RevisionList getRevisionList() {
		return revisionList;
	}
	
	public RevisionTree getRevisionTree() {
		return revisionTree;
	}

	public void close() {
		logger.info("FS {}: Closing archive",
				Util.formatArchiveId(archiveId));
		swarm.close();
		stopAdvertising();
	}
	
	public boolean isClosed() {
		return swarm.isClosed();
	}
	
	@Deprecated
	public void setSwarm(PeerSwarm swarm) {
		// test use only
		swarm.close();
		this.swarm = swarm;
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new InvalidArchiveConfigException();
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
	
	public void waitForPageReady(byte[] tag) throws IOException {
		String path = Page.pathForTag(tag);
		Stat stat = null;
		int attempts = 0;
		int maxAttempts = getMaster().getGlobalConfig().getInt("fs.settings.pageReadyMaxRetries");
		int delay = getMaster().getGlobalConfig().getInt("fs.settings.pageReadyRetryDelayMs");
		
		while(stat == null || stat.getSize() != this.getSerializedPageSize()) {
			if(++attempts >= maxAttempts) {
				if(stat == null) {
					throw new ENOENTException(path);
				}
				throw new InvalidPageException(path);
			} else if(attempts > 1) {
				Util.sleep(delay);
			}
			
			try {
				stat = storage.stat(path);
			} catch(ENOENTException exc) {}
		}
	}

	public byte[] readPageData(byte[] tag) throws IOException {
		return readPageData(tag, 0, this.getSerializedPageSize());
	}
	
	public byte[] readPageData(byte[] tag, int offset, int length) throws IOException {
		// ensure that a page is fully written to disk before reading it
		waitForPageReady(tag);
		
		try(File page = storage.open(Page.pathForTag(tag), File.O_RDONLY)) {
			page.seek(offset, File.SEEK_SET);
			return page.read(length);
		}
	}
}
