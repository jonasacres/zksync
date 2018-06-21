package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.utility.Util;

/** represents a fixed-size page of data from a file. handles encryption/decryption/storage of said page. */
public class Page {
	private ZKFile file; /** file handle from which this page was opened */
	protected int pageNum; /** zero-based page number */
	protected int size; /** size of actual page contents (may be less than fixed page size from filesystem) */
	private ByteBuffer contents; /** page contents */
	boolean dirty; /** true if page has been written to since last read/flush */
	
	public static String pathForTag(byte[] tag) {
	    StringBuilder sb = new StringBuilder();
	    for(int i = 0; i < tag.length; i++) {
	        sb.append(String.format("%02x", tag[i]));
	    	if(i < 2) sb.append("/");
	    }
	    
		return sb.toString();
	}
	
	public static byte[] tagForPath(String path) {
		return Util.hexToBytes(path.replace("/", ""));
	}
	
	public static byte[] expandTag(FS storage, long shortTag) throws IOException {
		String path = pathForTag(ByteBuffer.allocate(8).putLong(shortTag).array());
		String parent = storage.dirname(path);
		String basename = storage.basename(path);
		
		try {
			Directory dir = storage.opendir(parent);
			for(String subpath : dir.list()) {
				if(storage.basename(subpath).startsWith(basename)) {
					return tagForPath(Paths.get(parent, subpath).toString());
				}
			}
		} catch(ENOENTException exc) {}
		
		return null;
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
		
		byte[] pageTag = SignedSecureFile
		  .withParams(file.zkfs.archive.storage, textKey(), authKey(), file.zkfs.archive.config.privKey)
		  .write(plaintext.array(), file.zkfs.archive.config.pageSize);
		this.file.setPageTag(pageNum, pageTag);
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
	protected Key textKey() {
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
		ByteBuffer buf = ByteBuffer.allocate(20);
		buf.putLong(file.getInode().getStat().getInodeId()); // no dedupe between files
		buf.putLong(file.getInode().getIdentity()); // guard against key reuse when inodes are recycled
		buf.putInt(pageNum); // no dedupe within file
		return file.zkfs.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_PAGE, buf.array());
	}
	
	/** key used to produce page tag (provides authentication of page contents) */
	protected Key authKey() {
		return file.zkfs.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_PAGE);
	}
	
	/** load page contents from underlying storage */
	public void load() throws IOException {
		int pageSize = file.zkfs.archive.config.pageSize;
		
		byte[] pageTag = file.getPageTag(pageNum);
		if(pageNum == 0 && file.tree.numPages == 1 && pageTag.length < file.getFS().getArchive().getCrypto().hashLength()) {
			contents = ByteBuffer.allocate((int) file.zkfs.archive.config.pageSize);
			contents.put(file.inode.refTag.getLiteral());
			size = contents.position();
			return;
		}
		
		byte[] plaintext = SignedSecureFile
		  .withTag(pageTag, file.zkfs.archive.storage, textKey(), authKey(), file.zkfs.archive.config.pubKey)
		  .read();
		
		contents = ByteBuffer.allocate(pageSize);
		contents.put(plaintext);
		size = contents.position();
		dirty = false;
	}
	
	/** set page to empty (zero length, no contents) */
	public void blank() {
		contents = ByteBuffer.allocate(file.zkfs.archive.config.pageSize);
		size = 0;
		dirty = true;
	}
}
