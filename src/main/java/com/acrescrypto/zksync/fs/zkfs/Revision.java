package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;

/* Stores a revision of the archive. This is needed to bootstrap reading the archive.
 */
public class Revision {
	protected ArrayList<byte[]> parents; // revtags of parent revisions
	protected Inode supernode; // inode for inode table
	protected ZKFS fs; // ZKFS archive to which this revision belongs 
	protected byte[] revTag; // unique, non-confidential identifier for this revision
	
	public final static int REVISION_FILE_SIZE = 512;
	
	public static Revision activeRevision(ZKFS fs) throws IOException {
		try {
			return new Revision(fs, ZKFS.ACTIVE_REVISION);
		} catch(IOException|SecurityException e) {}
		
		Directory revdir;
		long bestMtime = 0;
		Revision bestRev = null;

		try {
			revdir = fs.getStorage().opendir(ZKFS.REVISION_DIR);
		} catch(ENOENTException e) {
			return null;
		}
		
		for(String revName : revdir.list()) {
			try {
				Revision rev = new Revision(fs, Paths.get(ZKFS.REVISION_DIR, revName).toString());
				long mtime = rev.supernode.getStat().getMtime();
				if(mtime > bestMtime || bestRev == null) {
					bestMtime = mtime;
					bestRev = rev;
				}
			} catch(SecurityException e) {
				// just ignore invalid rev files
			}
		}
		
		return bestRev;
	}
	
	// make a new revision based on the contents of an inode table
	public Revision(InodeTable table) {
		this.fs = table.getFS();
		this.supernode = table.getInode();
	}
	
	// read an existing revision
	public Revision(ZKFS fs, String path) throws IOException {
		this.fs = fs;
		read(path);
	}
	
	// declare the previous revision to this one so we can navigate history
	public void addParent(byte[] parent) {
		this.parents.add(parent);
	}
	
	// read and decrypt the contents of the revision file at path
	public void read(String path) throws IOException {
		byte[] ciphertext = fs.getStorage().read(path);
		setRevTagFromPath(path);
		if(!authKey().authenticate(ciphertext).equals(revTag)) throw new SecurityException();
		deserialize(textKey().wrappedDecrypt(ciphertext));
	}
	
	// write encrypted revision file to storage (path is dictated by revision contents and archive location)
	public void write() throws IOException {
		byte[] ciphertext = textKey().wrappedEncrypt(serialize(), REVISION_FILE_SIZE);
		this.revTag = authKey().authenticate(ciphertext);
		
		String path = ZKFS.REVISION_DIR + fs.pathForHash(this.revTag);
		fs.getStorage().write(path, ciphertext);
		fs.getStorage().squash(path);
	}
	
	public void makeActive() throws IOException {
		String revFile = ZKFS.REVISION_DIR + fs.pathForHash(this.revTag);
		// TODO: probably need to make parent directories...
		fs.getStorage().symlink(revFile, ZKFS.ACTIVE_REVISION);
	}
	
	// create plaintext serialized revision data (to be encrypted and written to storage)
	private byte[] serialize() {
		/*
		 *   parentTableLen       (int32, 4 bytes, total size of parent table in bytes)
		 *   parent table (parentTableLen/64 entries):
		 *     parentRevTag       (hash, 64 bytes)
		 *     (Total record size: 64 bytes)
		 *   supernodeText        (Inode, variable length)
		 */
		byte[] supernodeText = supernode.serialize();
		int parentTableLen = parents.size()*fs.getCrypto().hashLength();
		ByteBuffer buf = ByteBuffer.allocate(supernodeText.length + parentTableLen + 4);
		
		buf.putInt(parentTableLen);
		for(byte[] parent : parents) buf.put(parent);
		
		buf.put(supernodeText);
		
		return buf.array();
	}
	
	// load plaintext serialized revision data  
	private void deserialize(byte[] serialized) {
		this.parents = new ArrayList<byte[]>();
		
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		int parentTableLen = buf.getInt();
		for(int i = 0; i < parentTableLen/fs.getCrypto().hashLength(); i++) {
			byte[] parent = new byte[fs.getCrypto().hashLength()];
			buf.get(parent);
			parents.add(parent);
		}
		
		int supernodeLen = buf.getInt();
		buf.position(buf.position()-4);
		byte[] supernodeText = new byte[supernodeLen];
		buf.get(supernodeText);
		
		this.supernode = new Inode(fs);
	}
	
	// non-confidential name of this revision; used for storage location
	public byte[] getRevTag() {
		return this.revTag;
	}
	
	// extract revtag from a string path name
	private void setRevTagFromPath(String path) {
		// tag format is expected to be /path/to/rev/directory/12/34/abcdef...
		// concatenate last 3 elements to get hash
		
		String[] comps = path.split("/");
		if(comps.length < 3) throw new IllegalArgumentException(path + ": revision path does not appear to contain a hash");
		
		// take out the slashes
		String hashStr = comps[comps.length-3] + comps[comps.length-2] + comps[comps.length-1];
		if(hashStr.length() != 2*fs.getCrypto().hashLength()) throw new IllegalArgumentException(path + ": revision path does not contain a hash of the appropriate length");
		
		ByteBuffer revTagBuf = ByteBuffer.allocate(hashStr.length()/2);
		
		// convert from string to raw bytes
		for(int i = 0; i < revTagBuf.capacity(); i++) {
			byte r = 0;
			for(int j = 0; j < 2; j++) {
				r <<= 4;
				char c = hashStr.charAt(2*i+j);
				if(c >= 'a' && c <= 'f') c = (char) (c - 'a' + 0x0a);
				else if(c >= 'A' && c <= 'F') c = (char) (c - 'A' + 0x0a);
				else if(c >= '0' && c <= '9') c = (char) (c - '0');
				else throw new IllegalArgumentException(path + ": revision path contains non-hexadecimal characters (" + c + ")");
			}
			revTagBuf.put(r);
		}
		
		this.revTag = revTagBuf.array();
	}
	
	// key used to encrypt revision data
	private Key textKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_CIPHER, ZKFS.KEY_INDEX_PAGE_REVISION, revTag);
	}
	
	// key used to create revtag
	private Key authKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_AUTH, ZKFS.KEY_INDEX_PAGE_REVISION);
	}

	// Inode containing info about InodeTable
	public Inode getSupernode() {
		return supernode;
	}
}
