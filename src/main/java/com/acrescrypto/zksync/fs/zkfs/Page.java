package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SecureFile;

/** represents a fixed-size page of data from a file. handles encryption/decryption/storage of said page. */
public class Page {
	private ZKFile file; /** file handle from which this page was opened */
	protected int pageNum; /** zero-based page number */
	protected int size; /** size of actual page contents (may be less than fixed page size from filesystem) */
	private ByteBuffer contents; /** page contents */
	boolean dirty; /** true if page has been written to since last read/flush */
	
	/** path in underlying filesystem to a page identified by tag 
	 * @param archive */
	public static String pathForTag(ZKArchive archive, byte[] tag) {
		return archive.dataDir() + ZKFS.pathForHash(tag);
	}
	
	public static String pathForTag(byte[] archiveId, byte[] tag) {
		return ZKArchive.dataDirForArchiveId(archiveId) + ZKFS.pathForHash(tag);
	}
	
	/** initialize page object from file and page number */
	public Page(ZKFile file, int pageNum) throws IOException {
		this.file = file;
		this.pageNum = pageNum;
	}
	
	/** truncate page to a given size */
	public void truncate(int newSize) {
		if(size == newSize) return;
		size = newSize;
		contents.position(size);
		if(size < contents.capacity()) {
			contents.put(new byte[contents.capacity()-size]);
		}
		dirty = true;
	}
	
	/** write plaintext to page at current read/write pointer offset. must call finalize() to encrypt and write to storage. 
	    @throws IndexOutOfBoundsException content would exceed max page length
	 */
	public int write(byte[] plaintext, int offset, int length) throws IOException {
		contents.put(plaintext, offset, length);
		if(contents.position() > size) size = contents.position();
		dirty = true;
		return length;
	}
	
	/** write all buffered data to storage */
	public void flush() throws IOException {
		if(!dirty) return;
		dirty = false;
		
		ByteBuffer plaintext;
		
		if(size < contents.capacity()) {
			plaintext = ByteBuffer.allocate(size);
			plaintext.put(contents.array(), 0, size);
			
			// don't write immediates to disk; the pageTag = refTag = file contents
			if(pageNum == 0 && size < file.zkfs.archive.crypto.hashLength()) {
				file.setPageTag(pageNum, plaintext.array());
				return;
			}
		} else {
			plaintext = contents;
		}
		
		byte[] pageTag = this.authKey().authenticate(plaintext.array());
		this.file.setPageTag(pageNum, pageTag);
		
		byte[] authTag = authKey().authenticate(pageTag);
		SecureFile
		  .atPath(file.zkfs.archive.storage, pathForTag(file.zkfs.archive, authTag), textKey(pageTag), authTag, null)
		  .write(plaintext.array(), (int) file.zkfs.archive.keychain.pageSize);
	}
	
	/** read data from page into a supplied buffer
	 * 
	 * @param buf buffer into which to place read bytes
	 * @param offset offset in array in which to place first byte
	 * @param maxLength maximum number of bytes to read from page (will be less if end of page is reached before maxLength is reached)
	 * @return
	 */
	public int read(byte[] buf, int offset, int maxLength) {
		int readLen = (int) Math.min(maxLength, size-contents.position());
		contents.get(buf, offset, readLen);
		return readLen;
	}
	
	/** set read/write pointer offset  */
	public void seek(int offset) {
		contents.position(offset);
	}
	
	/** number of bytes between current read/write pointer offset and end of page contents */
	public int remaining() {
		return size - contents.position();
	}
	
	/** key used for encrypting page contents */
	protected Key textKey(byte[] pageTag) {
		return file.zkfs.archive.deriveKey(ZKArchive.KEY_TYPE_CIPHER, ZKArchive.KEY_INDEX_PAGE, pageTag);
	}
	
	/** key used to produce page tag (provides authentication of page contents and basis for page key) */
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
		return file.zkfs.archive.deriveKey(ZKArchive.KEY_TYPE_AUTH, ZKArchive.KEY_INDEX_PAGE, buf.array());
	}
	
	/** load page contents from underlying storage */
	public void load() throws IOException {
		int pageSize = (int) file.zkfs.archive.keychain.pageSize;
		
		if(file.inode.refTag.getRefType() == RefTag.REF_TYPE_IMMEDIATE) {
			assert(pageNum == 0);
			contents = ByteBuffer.allocate((int) file.zkfs.archive.keychain.pageSize);
			contents.put(file.inode.refTag.getLiteral());
			size = contents.position();
			return;
		}
		
		byte[] pageTag = file.getPageTag(pageNum);
		byte[] authTag = authKey().authenticate(pageTag);
		byte[] plaintext = SecureFile
		  .atPath(file.zkfs.archive.storage, pathForTag(file.zkfs.archive, authTag), textKey(pageTag), authTag, null)
		  .read();
		
		if(!Arrays.equals(this.authKey().authenticate(plaintext), pageTag)) {
			throw new SecurityException();
		}
		
		contents = ByteBuffer.allocate(pageSize);
		contents.put(plaintext);
		size = contents.position();
		dirty = false;
	}
	
	/** set page to empty (zero length, no contents) */
	public void blank() {
		contents = ByteBuffer.allocate((int) file.zkfs.archive.keychain.pageSize);
		size = 0;
		dirty = true;
	}
}
