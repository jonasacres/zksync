package com.acrescrypto.zksync.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

public class KeyFile {
	private ZKArchive archive;
	private Key textRoot, hashRoot;
	
	public KeyFile(ZKArchive archive, char[] passphrase) throws IOException {
		this.archive = archive;
		read(passphrase);
	}
	
	private Key makePassphraseKey(char[] passphrase) {
		byte[] ppBytes = new byte[passphrase.length];
		for(int i = 0; i < passphrase.length; i++) ppBytes[i] = (byte) passphrase[i];
		return new Key(archive.getCrypto(), archive.getCrypto().deriveKeyFromPassword(ppBytes, "zksync-salt".getBytes()));
	}
	
	public void read(char[] passphrase) throws IOException {
		Key ppKey = makePassphraseKey(passphrase);
		
		try {
			int len = archive.getCrypto().symKeyLength();
			byte[] plaintext = SecureFile
					.atPath(archive.getStorage(), getPath(), ppKey, new byte[0], null)
					.read();
			byte[][] rawKeys = new byte[2][len];
			for(int i = 0; i < 2*len; i++) rawKeys[i/len][i % len] = plaintext[i];
			
			this.setCipherRoot(new Key(archive.getCrypto(), rawKeys[0]));
			this.setHashRoot(new Key(archive.getCrypto(), rawKeys[1]));
		} catch(IOException ex) {
			generate(ppKey);
			write(passphrase); // TODO: this is kind of awkward, since there's no way to open-and-fail.
		}
	}
	
	public void generate(Key ppKey) {
		this.setCipherRoot(ppKey.derive(ZKArchive.KEY_TYPE_CIPHER, new byte[0]));
		this.setHashRoot(ppKey.derive(ZKArchive.KEY_TYPE_AUTH, new byte[0]));
	}
	
	public void write(char[] passphrase) throws IOException {
		Key ppKey = makePassphraseKey(passphrase);
		int len = archive.getCrypto().symKeyLength();
		
		ByteBuffer plaintext = ByteBuffer.allocate(2*len);
		plaintext.put(this.getCipherRoot().getRaw());
		plaintext.put(this.getAuthRoot().getRaw());
		SecureFile.atPath(archive.getStorage(), getPath(), ppKey, new byte[0], null).write(plaintext.array(), 0);
	}
	
	public String getPath() {
		return ".zksync/keyfile";
	}

	public Key getCipherRoot() {
		return textRoot;
	}

	public void setCipherRoot(Key textRoot) {
		this.textRoot = textRoot;
	}

	public Key getAuthRoot() {
		return hashRoot;
	}

	public void setHashRoot(Key hashRoot) {
		this.hashRoot = hashRoot;
	}
}
