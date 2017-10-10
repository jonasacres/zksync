package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;

// represents a fixed-size page of data from a file. handles encryption/decryption/storage of said page.
public class Page {
	private ZKFile file;
	private int pageNum;
	private ByteBuffer writeBuffer;
	
	public Page(ZKFile file, int index) {
		this.file = file;
		this.pageNum = index;
	}
	
	/* append plaintext to page. must call finalize() to encrypt and write to storage. 
	 * @throws IndexOutOfBoundsException content would exceed max page length
	 */
	public void append(byte[] contents, int offset) throws IOException {
		if(writeBuffer == null) {
			writeBuffer = ByteBuffer.allocate((int) file.getFS().getPrivConfig().getPageSize());
			writeBuffer.put(read()); // add in current page contents, if any
			if(offset >= 0) writeBuffer.position(offset);
		}
		
		if(writeBuffer.position() + contents.length > file.getFS().getPrivConfig().getPageSize()) {
			throw new IndexOutOfBoundsException();
		}
		
		writeBuffer.put(contents);
	}
	
	// set plaintext contents of page (replacing existing content), encrypt and write to storage.
	public void setPlaintext(byte[] plaintext) throws InaccessibleStorageException {
		long pageSize = file.getFS().getPrivConfig().getPageSize();
		if(plaintext.length < pageSize) {
			byte[] padded = new byte[(int) pageSize];
			for(int i = 0; i < pageSize; i++) {
				padded[i] = i >= plaintext.length ? 0 : plaintext[i];
			}
		} else if(plaintext.length > pageSize) {
			throw new IndexOutOfBoundsException();
		}
		 
		byte[] pageTag = this.authKey().authenticate(plaintext);
		byte[] ciphertext = this.textKey(pageTag).wrappedEncrypt(plaintext, (int) file.getFS().getPrivConfig().getPageSize());
		this.file.setPageTag(pageNum, pageTag);
		
		try {
			String path = ZKFS.DATA_DIR + file.getFS().pathForHash(pageTag);
			file.getFS().getStorage().write(path, ciphertext);
			file.getFS().getStorage().squash(path); 
		} catch (IOException e) {
			throw new InaccessibleStorageException();
		}
		writeBuffer = null;
	}
	
	// write all buffered data to storage
	public void finalize() throws InaccessibleStorageException {
		if(writeBuffer == null) return;
		setPlaintext(writeBuffer.array());
	}
	
	protected Key textKey(byte[] pageTag) {
		return file.getFS().deriveKey(ZKFS.KEY_TYPE_CIPHER, ZKFS.KEY_INDEX_PAGE, pageTag);
	}
	
	protected Key authKey() {
		// this is designed to intentionally prevent data deduplication
		// rationale: deduplication creates a chosen plaintext attack revealing whether data was previously stored
		// attacker must have access to archive size and ability to get keyholder to inject arbitrary data
		// e.g. victim backs up home directory with zksync automatically, including e-mail folder
		//   attacker can view encrypted backups, and also knows victim's e-mail address
		//   attacker believes victim may be participant of intercepted plan described in PlanToKillTheEvilKing.pdf
		//   attacker notes archive size = X
		//   attacker e-mails PlanToKillTheEvilKing.pdf to victim
		//   victim receives PlanToKillTheEvilKing.pdf e-mail, stored on hard disk, added to zksync without any operator input
		//   attacker notes archive size did not increase by size of PlanToKillTheEvilKing.pdf
		//   victim does not pass go, does not collect $200
		
		// without further ado, let's go about the nasty business of killing the feature other content-addressable
		// encrypted file storage systems like to brag about.
		
		ByteBuffer buf = ByteBuffer.allocate(12);
		buf.putLong(file.getInode().getStat().getInodeId()); // no dedupe between files
		buf.putInt(pageNum); // no dedupe within file
		return file.getFS().deriveKey(ZKFS.KEY_TYPE_AUTH, ZKFS.KEY_INDEX_PAGE, buf.array());
	}
	
	/* reads and decrypts the page
	 * @return full plaintext of page
	 */
	public byte[] read() throws IOException {
		byte[] pageTag = file.getPageTag(pageNum);
		byte[] plaintext, ciphertext;
		String path = ZKFS.DATA_DIR + file.getFS().pathForHash(pageTag);
		
		try {
			ciphertext = file.getFS().getStorage().read(path);
		} catch(IOException exc) {
			throw new InvalidArchiveException(exc.toString());
		}
		plaintext = textKey(pageTag).wrappedDecrypt(ciphertext);
		
		// i have two minds about this. paranoia is good, and checking consistency of state is smart.
		// downside: hashing every page we read is added cost, and this check seems redundant with AEAD cipher
		if(!this.authKey().authenticate(plaintext).equals(pageTag)) throw new SecurityException();
		
		return plaintext;
	}
}
