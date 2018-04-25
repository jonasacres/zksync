package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;

public class Keychain {
	
	public final static int KEYFILE_MAGIC = 0x6CF2AA14;
	public final static int KEYFILE_SECTION_ARCHIVE_INFO = 0x0001;
	
	protected byte[] archiveId;
	protected Key passphraseRoot;
	
	protected Key seedRoot;
	protected Key seedId;
	protected Key seedRegId;
	
	protected Key configFileKey;
	protected byte[] configFileIv;
	protected byte[] configFileTag;
	
	protected Key authRoot;
	protected Key textRoot;
	
	protected ZKMaster master;
	
	// TODO: Keychain might be a misnomer. ArchiveConfig might be better.
	// TODO: How does a keychain get created?
	
	// NOTE: in theory we support pageSize > MAX_INT, but in practice several operations cast to int and need a refactor
	// for this to work. TODO: guard against trying to load archives with long-valued page sizes.
	protected long pageSize;
	protected String description;
	
	/** Read an existing archive. */
	public Keychain(ZKMaster master, Key key, byte[] archiveId, boolean isSeedKey) {
		this.master = master;
		this.pageSize = -1;
		
		if(isSeedKey) {
			deriveFromPassphraseRoot(key);
		} else {
			deriveFromSeedRoot(key);
		}
		
		try {
			read();
		} catch (IOException e) {
			// TODO: what if we don't have the key file yet?
		}
	}
	
	/** Create a new archive. */
	public Keychain(ZKMaster master, Key passphraseRoot, String description, int pageSize) {
		// TODO: need to establish public key for signing here as well
		this.master = master;
		this.pageSize = pageSize;
		this.description = description;
		deriveFromPassphraseRoot(passphraseRoot);
		initRoots();
	}
	
	public boolean isSeedOnly() {
		return passphraseRoot != null;
	}
	
	public void isConfigAvailable() {
		master.storage.exists(Page.pathForTag(archiveId, configFileTag));
	}
	
	public boolean isConfigLoaded() {
		return authRoot != null;
	}
	
	public void parseFile(ByteBuffer contents) {
		assertState(contents.getLong() == KEYFILE_MAGIC);
		while(contents.hasRemaining()) {
			int type = Util.unsignShort(contents.getShort());
			int length = Util.unsignShort(contents.getShort());
			int expectedIndex = contents.position() + length;
			
			assertState(contents.remaining() >= length);
			switch(type) {
			case KEYFILE_SECTION_ARCHIVE_INFO:
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
		
		byte[] textRootRaw = new byte[master.crypto.symKeyLength()];
		byte[] authRootRaw = new byte[master.crypto.symKeyLength()];
		
		contents.get(textRootRaw);
		contents.get(authRootRaw);

		textRoot = new Key(this.master.crypto, textRootRaw);
		authRoot = new Key(this.master.crypto, authRootRaw);
	}
	
	public byte[] getArchiveId() {
		return new byte[master.crypto.hashLength()];
		// TODO: return archiveId;
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
		
		master.storage.write(Page.pathForTag(archiveId, configFileTag), writeBuf.array());
	}
	
	public void read() throws IOException {
		byte[] ciphertext = master.storage.read(Page.pathForTag(archiveId, configFileTag));
		byte[] serialized = configFileKey.decrypt(configFileIv, ciphertext);
		deserialize(serialized);
	}
	
	protected byte[] serialize() {
		byte[] descString = description.getBytes();
		int headerSize = 4; // magic
		int sectionHeaderSize = 2 + 4; // section_type + length
		int archiveInfoSize = 8 + textRoot.getRaw().length + authRoot.getRaw().length + descString.length; // pageSize + textRoot + authRoot + description
		
		assertState(descString.length <= Short.MAX_VALUE);
		
		ByteBuffer buf = ByteBuffer.allocate(headerSize+sectionHeaderSize+archiveInfoSize);
		buf.putLong(KEYFILE_MAGIC);
		buf.putShort((short) KEYFILE_SECTION_ARCHIVE_INFO);
		buf.putInt((short) archiveInfoSize);
		buf.putLong(pageSize);
		buf.put(textRoot.getRaw());
		buf.put(authRoot.getRaw());
		buf.put(descString);
		
		assertState(!buf.hasRemaining());
		
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		assertState(buf.getLong() == KEYFILE_MAGIC);
		while(buf.hasRemaining()) {
			assertState(buf.remaining() >= 6); // 2-byte type + 4-byte length
			int type = Util.unsignShort(buf.getShort());
			int length = buf.getInt();
			assertState(length >= 8 + 2*master.crypto.symKeyLength());
			assertState(buf.remaining() >= length);
			if(type != KEYFILE_SECTION_ARCHIVE_INFO) {
				// only support one record type in this version...
				buf.position(buf.position() + length);
				continue;
			}
			
			this.pageSize = buf.getLong();
			byte[] textRootRaw = new byte[master.crypto.symKeyLength()],
				   authRootRaw = new byte[master.crypto.symKeyLength()];
			this.textRoot = new Key(master.crypto, textRootRaw);
			this.authRoot = new Key(master.crypto, authRootRaw);
			byte[] descriptionRaw = new byte[length - 8 + 2*master.crypto.symKeyLength()];
			buf.get(descriptionRaw);
			this.description = new String(descriptionRaw);
			break;
		}
	}
	
	protected void initRoots() {
		authRoot = new Key(master.crypto, master.crypto.rng(master.crypto.symKeyLength()));
		textRoot = new Key(master.crypto, master.crypto.rng(master.crypto.symKeyLength()));
		configFileIv = master.crypto.rng(master.crypto.symIvLength());
		calculateArchiveId();
	}
	
	protected void deriveFromPassphraseRoot(Key passphraseRoot) {
		this.passphraseRoot = passphraseRoot;
		deriveFromSeedRoot(passphraseRoot.derive(0x00, new byte[0]));		
		configFileKey = passphraseRoot.derive(0x01, new byte[0]);
		configFileTag = passphraseRoot.derive(0x02, new byte[0]).getRaw();
	}
	
	protected void deriveFromSeedRoot(Key seedRoot) {
		this.seedRoot = seedRoot;
		seedId = seedRoot.derive(0x00, new byte[0]);
		seedRegId = seedRoot.derive(0x01, new byte[0]);
	}
	
	protected Key keyFileTextKey(byte[] passphrase) {
		return new Key(master.crypto, master.crypto.deriveKeyFromPassphrase(passphrase, "zksync-salt".getBytes()));
	}
	
	protected Key temporalSeedId(int offset) {
		return temporalSeedDerivative(0x00, offset);
	}
	
	protected Key temporalSeedKey(int offset) {
		return temporalSeedDerivative(0x01, offset);
	}
	
	protected Key temporalSeedDerivative(int index, int offset) {
		ByteBuffer timeTweak = ByteBuffer.allocate(8);
		timeTweak.putLong(timeSlice(offset));
		return seedId.derive(index, timeTweak.array());
	}
	
	protected long timeSlice(int offset) {
		return timeSliceInterval() * (System.currentTimeMillis()/timeSliceInterval() + offset);
	}
	
	protected int timeSliceInterval() {
		return 1000*60*60*3; // 3 hours
	}
	
	protected void calculateArchiveId() {
		ByteBuffer keyMaterialBuf = ByteBuffer.allocate(authRoot.getRaw().length + textRoot.getRaw().length);
		keyMaterialBuf.put(authRoot.getRaw());
		keyMaterialBuf.put(textRoot.getRaw());
		assertState(!keyMaterialBuf.hasRemaining());
		archiveId = passphraseRoot.authenticate(keyMaterialBuf.array());
		// TODO: needs to include signing key
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new RuntimeException();
	}
}
