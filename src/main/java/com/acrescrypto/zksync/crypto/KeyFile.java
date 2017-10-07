package com.acrescrypto.zksync.crypto;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class KeyFile {
	Ciphersuite suite;
	String path;
	Key textRoot, hashRoot, ppKey;
	
	KeyFile(String path, Ciphersuite suite, char[] passphrase) {
		this.suite = suite;
		this.path = path;
		this.ppKey = new Key(suite, passphrase);
		read();
	}
	
	void read() {
		File file = null;
		FileInputStream fis = null;
		
		try {
			file = new File(this.path);
			byte[] ciphertext = new byte[(int) file.length()];
			fis = new FileInputStream(file);
			fis.read(ciphertext);

			byte[] plaintext = this.ppKey.wrappedDecrypt(ciphertext);
			byte[][] rawKeys = new byte[2][suite.symKeyLength()];
			for(int i = 0; i < 2*suite.symKeyLength(); i++) {
				rawKeys[i/suite.symKeyLength()][i % suite.symKeyLength()] = plaintext[i];
			}
			
			this.textRoot = new Key(suite, rawKeys[0]);
			this.hashRoot = new Key(suite, rawKeys[1]);
		} catch(IOException ex) {
			generate();
		} finally {
			try {
				if(fis != null) {
					fis.close();
				}
			} catch(IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	void generate() {
		this.textRoot = new Key(suite);
		this.hashRoot = new Key(suite);
	}
	
	void write() {
		byte[] plaintext = new byte[2*suite.symKeyLength()];
		
		for(int i = 0; i < suite.symKeyLength(); i++) {
			plaintext[i] = this.textRoot.getRaw()[i];
		}
		
		for(int i = 0; i < suite.symKeyLength(); i++) {
			plaintext[suite.symKeyLength() + i] = this.textRoot.getRaw()[i];
		}
		
		byte[] ciphertext = ppKey.wrappedEncrypt(plaintext, 0);
		FileOutputStream fos = null;
		
		try {
			fos = new FileOutputStream("pathname");
			fos.write(ciphertext);
		} catch(IOException ex) {
			ex.printStackTrace();
		} finally {
			try {
				if(fos != null) {
					fos.close();
				}
			} catch(IOException ex) {
				ex.printStackTrace();
			}
		}
	}
}
