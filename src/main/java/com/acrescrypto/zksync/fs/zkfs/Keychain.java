package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;

public class Keychain {
	
	public final static int KEYFILE_MAGIC = 0x6CF2AA14;
	public final static int KEYFILE_SECTION_ARCHIVE_INFO = 0x0001;
	
	protected byte[] archiveId;
	protected Key seedRoot;
	protected Key seedId;
	protected Key seedRegId;
	
	protected Key configFileKey;
	protected byte[] configFileIv;
	protected byte[] configFileTag;
	
	protected Key authRoot;
	protected Key textRoot;
	
	protected ZKArchive archive;
	
	protected long pageSize;
	public String description;
	
	public Keychain(ZKArchive archive, byte[] passphrase) throws IOException {
		byte[] passphraseRootRaw = archive.crypto.deriveKeyFromPassword(passphrase, "zksync-salt".getBytes());
		Key passphraseRoot = new Key(archive.crypto, passphraseRootRaw);
		
		this.archive = archive;
		derive(passphraseRoot);
		
		archive.keychain = this;
		
		// TODO: read file?
		// parseFile(ByteBuffer.wrap(config.read()));
		initRoots();
		calculateArchiveId(passphraseRoot);
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
		
		byte[] textRootRaw = new byte[archive.crypto.symKeyLength()];
		byte[] authRootRaw = new byte[archive.crypto.symKeyLength()];
		
		contents.get(textRootRaw);
		contents.get(authRootRaw);

		textRoot = new Key(this.archive.crypto, textRootRaw);
		authRoot = new Key(this.archive.crypto, authRootRaw);
	}
	
	public byte[] getArchiveId() {
		return new byte[archive.crypto.hashLength()];
		// TODO: return archiveId;
	}
	
	protected void write() throws IOException {
		ByteBuffer writeBuf = ByteBuffer.allocate((int) pageSize);
		writeBuf.put(configFileIv);
		byte[] ciphertext = configFileKey.encrypt(configFileIv, buildPlaintext(), (int) pageSize - configFileIv.length);
		writeBuf.put(ciphertext);
		assertState(!writeBuf.hasRemaining());
		archive.storage.write(Page.pathForTag(archive, configFileTag), writeBuf.array());
	}
	
	protected byte[] buildPlaintext() {
		byte[] descString = description.getBytes();
		int headerSize = 4;
		int sectionHeaderSize = 2 + 2;
		int archiveInfoSize = 8 + 2 + descString.length + textRoot.getRaw().length + authRoot.getRaw().length;
		
		ByteBuffer buf = ByteBuffer.allocate(headerSize+sectionHeaderSize+archiveInfoSize);
		buf.putLong(KEYFILE_MAGIC);
		buf.putLong(KEYFILE_SECTION_ARCHIVE_INFO);
		buf.putLong(pageSize);
		buf.putShort((short) archiveInfoSize);
		buf.put(descString);
		buf.put(textRoot.getRaw());
		buf.put(authRoot.getRaw());
		
		assertState(!buf.hasRemaining());
		
		return buf.array();
	}
	
	protected void initRoots() {
		authRoot = new Key(archive.crypto, archive.crypto.rng(archive.crypto.symKeyLength()));
		textRoot = new Key(archive.crypto, archive.crypto.rng(archive.crypto.symKeyLength()));
		configFileIv = archive.crypto.rng(archive.crypto.symIvLength());
	}
	
	protected void derive(Key passphraseRoot) {		
		seedRoot = passphraseRoot.derive(0x00, new byte[0]);
		seedId = seedRoot.derive(0x00, new byte[0]);
		seedRegId = seedRoot.derive(0x01, new byte[0]);
		
		configFileKey = passphraseRoot.derive(0x01, new byte[0]);
		configFileTag = passphraseRoot.derive(0x02, new byte[0]).getRaw();
	}
	
	protected Key keyFileTextKey(byte[] passphrase) {
		return new Key(archive.crypto, archive.crypto.deriveKeyFromPassword(passphrase, "zksync-salt".getBytes()));
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
	
	protected void calculateArchiveId(Key passphraseRoot) {
		ByteBuffer keyMaterialBuf = ByteBuffer.allocate(authRoot.getRaw().length + textRoot.getRaw().length);
		keyMaterialBuf.put(authRoot.getRaw());
		keyMaterialBuf.put(textRoot.getRaw());
		assertState(!keyMaterialBuf.hasRemaining());
		archiveId = passphraseRoot.authenticate(keyMaterialBuf.array());
	}
	
	protected void assertState(boolean state) {
		if(!state) throw new RuntimeException();
	}
}
