package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.*;

// represents a fixed-size page of data from a file. handles encryption/decryption/storage of said page.
public class Page {
	private ZKFile file;
	protected int pageNum, size;
	private ByteBuffer contents;
	boolean dirty;
	
	public static String pathForTag(byte[] tag) {
		return ZKArchive.DATA_DIR + ZKFS.pathForHash(tag);
	}
	
	public Page(ZKFile file, int index) throws IOException {
		this.file = file;
		this.pageNum = index;
	}
	
	public void truncate(int newSize) {
		if(size == newSize) return;
		size = newSize;
		contents.position(size);
		if(size < contents.capacity()) {
			contents.put(new byte[contents.capacity()-size]);
		}
		dirty = true;
	}
	
	/* append plaintext to page. must call finalize() to encrypt and write to storage. 
	 * @throws IndexOutOfBoundsException content would exceed max page length
	 */
	public int write(byte[] plaintext, int offset, int length) throws IOException {
		contents.put(plaintext, offset, length);
		if(contents.position() > size) size = contents.position();
		dirty = true;
		return length;
	}
	
	// write all buffered data to storage
	public void flush() throws IOException {
		if(!dirty) return;
		dirty = false;
		
		ByteBuffer plaintext;
		
		if(size < contents.capacity()) {
			plaintext = ByteBuffer.allocate(size);
			plaintext.put(contents.array(), 0, size);
			
			// don't write immediates to disk; the pageTag = refTag = file contents
			if(pageNum == 0 && size < file.fs.archive.crypto.hashLength()) {
				file.setPageTag(pageNum, plaintext.array());
				return;
			}
		} else {
			plaintext = contents;
		}
		
		byte[] pageTag = this.authKey().authenticate(plaintext.array());
		byte[] ciphertext = this.textKey(pageTag).wrappedEncrypt(plaintext.array(), (int) file.fs.archive.privConfig.getPageSize());
		this.file.setPageTag(pageNum, pageTag);
		
		try {
			String path = pathForTag(authKey().authenticate(pageTag));
			file.fs.archive.storage.write(path, ciphertext);
			file.fs.archive.storage.squash(path); 
		} catch (IOException e) {
			throw new InaccessibleStorageException();
		}
	}
	
	public int read(byte[] buf, int offset, int maxLength) {
		int readLen = (int) Math.min(maxLength, size-contents.position());
		contents.get(buf, offset, readLen);
		return readLen;
	}
	
	public void seek(int offset) {
		contents.position(offset);
	}
	
	public int remaining() {
		return size - contents.position();
	}
	
	protected Key textKey(byte[] pageTag) {
		return file.fs.archive.deriveKey(ZKArchive.KEY_TYPE_CIPHER, ZKArchive.KEY_INDEX_PAGE, pageTag);
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
		return file.fs.archive.deriveKey(ZKArchive.KEY_TYPE_AUTH, ZKArchive.KEY_INDEX_PAGE, buf.array());
	}
	
	public void load() throws IOException {
		int pageSize = file.fs.archive.privConfig.getPageSize();
		long totalSize = file.getStat().getSize();
		boolean isLastPage = pageNum == totalSize/pageSize; 
		size = (int) (isLastPage ? totalSize % pageSize : pageSize);
		
		if(pageNum == 0 && size < file.fs.archive.crypto.hashLength()) {
			contents = ByteBuffer.allocate(file.fs.archive.privConfig.getPageSize());
			if(size > 0) {
				file.getInode().getRefTag().setLiteral(contents.array(), 0, size);
			}
			return;
		}
		
		byte[] pageTag = file.getPageTag(pageNum);
		byte[] ciphertext;
		String path = pathForTag(authKey().authenticate(pageTag));
		
		try {
			ciphertext = file.fs.archive.storage.read(path);
		} catch(ENOENTException exc) {
			if(size > 0) throw new ENOENTException(path);
			contents = ByteBuffer.allocate(size);
			return;
		} catch(IOException exc) {
			throw new InvalidArchiveException(exc.toString());
		}
		
		byte[] plaintext = textKey(pageTag).wrappedDecrypt(ciphertext);
		
		if(!Arrays.equals(this.authKey().authenticate(plaintext), pageTag)) {
			throw new SecurityException();
		}
		
		contents = ByteBuffer.allocate(pageSize);
		contents.put(plaintext);
	}
	
	public void blank() {
		contents = ByteBuffer.allocate(file.fs.archive.privConfig.getPageSize());
		size = 0;
		dirty = false;
	}
}
