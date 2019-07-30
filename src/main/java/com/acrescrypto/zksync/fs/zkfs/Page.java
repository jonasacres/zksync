package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.utility.Util;

/** represents a fixed-size page of data from a file. handles encryption/decryption/storage of said page. */
public class Page {
	private ZKFile file; /** file handle from which this page was opened */
	protected int pageNum; /** zero-based page number */
	protected int size; /** size of actual page contents (may be less than fixed page size from filesystem) */
	private ByteBuffer contents; /** page contents */
	boolean dirty; /** true if page has been written to since last read/flush */
	protected Logger logger = LoggerFactory.getLogger(Page.class);
	
	public static StorageTag expandTag(CryptoSupport crypto, FS storage, long shortTag) throws IOException {
		StorageTag sTag = new StorageTag(crypto, Util.serializeLong(shortTag));
		String path = sTag.path();
		String parent = storage.dirname(path);
		String basename = storage.basename(path);
		Directory dir = null;
		
		try {
			dir = storage.opendir(parent);
			for(String subpath : dir.list()) {
				if(storage.basename(subpath).startsWith(basename)) {
					String qualifiedSubpath = Paths.get(parent, subpath).toString();
					return new StorageTag(crypto, qualifiedSubpath);
				}
			}
		} catch(ENOENTException exc) {
			// ignore
		} finally {
			if(dir != null) {
				dir.close();
			}
		}
		
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
		logger.trace("ZKFS {} {}: {} page {} truncate to size {}, was {}",
				Util.formatArchiveId(file.zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(file.zkfs.getBaseRevision()),
				file.getPath(),
				pageNum,
				newSize,
				size);
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
		logger.trace("ZKFS {} {}: {} page {} write offset {} length {}, newSize={}, size={}",
				Util.formatArchiveId(file.zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(file.zkfs.getBaseRevision()),
				file.getPath(),
				pageNum,
				offset,
				length,
				contents.position(),
				size);
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
				StorageTag immediate = new StorageTag(file.getFS().getArchive().getCrypto(),
						plaintext.array());
				file.setPageTag(pageNum, immediate);
				return;
			}
		} else {
			plaintext = contents;
		}
		
		StorageTag pageTag = SignedSecureFile
		  .withParams(file.zkfs.archive.storage, textKey(), saltKey(), authKey(), file.zkfs.archive.config.privKey)
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
		int readLen = (int) Math.min(maxLength, Math.max(0, size-contents.position()));
		logger.trace("ZKFS {} {}: (PAGE READ) {} page {} buffer, pageOffset={}, pageMaxLength={}, pageSize={}, size={}, position={}, readLen={}, tag={}",
				Util.formatArchiveId(file.zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(file.zkfs.baseRevision),
				file.path,
				pageNum,
				offset,
				maxLength,
				contents.limit(),
				size,
				contents.position(),
				readLen);
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
		byte[] archiveId = file.getFS().getArchive().getConfig().getArchiveId();
		ByteBuffer buf = ByteBuffer.allocate(8 + 4 + archiveId.length);
		buf.putLong(file.getInode().getIdentity()); // no dedupe between files (do not use inode id, breaks diff merges)
		buf.putInt(pageNum); // no dedupe within file
		buf.put(file.getFS().getArchive().getConfig().getArchiveId());
		
		return file.zkfs.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-text-key",
				buf.array());
	}
	
	/** used with text key to encrypt page contents */
	protected Key saltKey() {
		byte[] archiveId = file.getFS().getArchive().getConfig().getArchiveId();
		ByteBuffer buf = ByteBuffer.allocate(8 + 4 + archiveId.length);
		buf.putLong(file.getInode().getIdentity()); // no dedupe between files (do not use inode id, breaks diff merges)
		buf.putInt(pageNum); // no dedupe within file
		buf.put(file.getFS().getArchive().getConfig().getArchiveId());
		
		return file.zkfs.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-salt-key",
				buf.array());
	}
	
	/** key used to produce page tag (provides authentication of page contents) */
	protected Key authKey() {
		return file.zkfs.archive.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED,
				"easysafe-page-auth-key");
	}
	
	/** load page contents from underlying storage */
	public void load() throws IOException {
		int pageSize = file.zkfs.archive.config.pageSize;
		contents = ByteBuffer.allocate(pageSize);
		
		StorageTag pageTag = file.getPageTag(pageNum);
		if(pageNum == 0 && file.tree.numPages == 1 && pageTag.isImmediate()) {
			contents.put(file.inode.getRefTag().getStorageTag().getTagBytes());
		} else {
			if(file.getFS().getArchive().getStorage() instanceof BackedFS) {
				// make sure the page is ready if this is a non-cached filesystem
				file.getFS().getArchive().getConfig().waitForPageReady(pageTag,
						file.getFS().getReadTimeoutMs());
			}
			
			try {
				byte[] plaintext = SignedSecureFile
				  .withTag(pageTag, file.zkfs.archive.storage, textKey(), saltKey(), authKey(), file.zkfs.archive.config.pubKey)
				  .read(!file.trusted);
				contents.put(plaintext);
			} catch(SecurityException exc) {
				throw exc;
			}
		}
		
		size = contents.position();
		logger.trace("ZKFS {} {}: Page {} ({}) of {} has {} bytes",
				Util.formatArchiveId(file.zkfs.getArchive().getConfig().getArchiveId()),
				Util.formatRevisionTag(file.zkfs.getBaseRevision()),
				pageNum,
				pageTag,
				file.getPath(),
				size);
		dirty = false;
	}
	
	/** set page to empty (zero length, no contents) */
	public void blank() {
		contents = ByteBuffer.allocate(file.zkfs.archive.config.pageSize);
		size = 0;
		dirty = true;
	}
}
