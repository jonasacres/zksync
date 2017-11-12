package com.acrescrypto.zksync.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.fs.zkfs.ZKFS;

public class KeyFile {
	private ZKFS fs;
	private Key textRoot, hashRoot;
	
	public KeyFile(ZKFS fs, char[] passphrase) {
		this.fs = fs;
		read(passphrase);
	}
	
	private Key makePassphraseKey(char[] passphrase) {
		byte[] ppBytes = new byte[passphrase.length];
		for(int i = 0; i < passphrase.length; i++) ppBytes[i] = (byte) passphrase[i];
		return new Key(fs.getArchive().getCrypto(), fs.getArchive().getCrypto().deriveKeyFromPassword(ppBytes, "zksync-salt".getBytes()));
	}
	
	public void read(char[] passphrase) {
		Key ppKey = makePassphraseKey(passphrase);
		
		try {
			int len = fs.getArchive().getCrypto().symKeyLength();
			byte[] ciphertext = fs.getArchive().getStorage().read(getPath());
			byte[] plaintext = ppKey.wrappedDecrypt(ciphertext);
			byte[][] rawKeys = new byte[2][len];
			for(int i = 0; i < 2*len; i++) rawKeys[i/len][i % len] = plaintext[i];
			
			this.setCipherRoot(new Key(fs.getArchive().getCrypto(), rawKeys[0]));
			this.setHashRoot(new Key(fs.getArchive().getCrypto(), rawKeys[1]));
		} catch(IOException ex) {
			generate();
			write(passphrase); // TODO: this is kind of awkward, since there's no way to open-and-fail.
		}
	}
	
	public void generate() {
		this.setCipherRoot(new Key(fs.getArchive().getCrypto()));
		this.setHashRoot(new Key(fs.getArchive().getCrypto()));
	}
	
	public void write(char[] passphrase) {
		Key ppKey = makePassphraseKey(passphrase);
		int len = fs.getArchive().getCrypto().symKeyLength();
		
		ByteBuffer plaintext = ByteBuffer.allocate(2*len);
		plaintext.put(this.getCipherRoot().getRaw());
		plaintext.put(this.getAuthRoot().getRaw());
		byte[] ciphertext = ppKey.wrappedEncrypt(plaintext.array(), 0);
		
		try {
			fs.getArchive().getStorage().write(getPath(), ciphertext);
		} catch(IOException ex) {
			ex.printStackTrace();
		}
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
