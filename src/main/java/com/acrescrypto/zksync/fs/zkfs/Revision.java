package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;

/* Stores a revision of the archive. This is needed to bootstrap reading the archive.
 */
public class Revision {
	protected ArrayList<RevisionTag> parents = new ArrayList<RevisionTag>(); // revtags of parent revisions
	protected Inode supernode; // inode for inode table
	protected ZKFS fs; // ZKFS archive to which this revision belongs 
	protected RevisionTag tag; // unique, non-confidential identifier for this revision
	
	public final static int REVISION_FILE_SIZE = 512;
	
	public static Revision activeRevision(ZKFS fs) throws IOException {
		// TODO: attempt to load selected revision
		// TODO: identify default revision if no selection is made
		
		return null;
	}
	
	// make a new revision based on the contents of an inode table
	public Revision(InodeTable table) {
		this.fs = table.getFS();
		this.supernode = table.getInode();
	}
	
	// read an existing revision
	public Revision(RevisionTag tag) throws IOException {
		this.fs = tag.fs;
		read(tag);
	}
	
	// declare the previous revision to this one so we can navigate history
	public void addParent(RevisionTag parentTag) {
		this.parents.add(parentTag);
	}
	
	// read and decrypt the contents of the revision file at path
	public void read(RevisionTag tag) throws IOException {
		byte[] ciphertext = fs.getStorage().read(tag.getPath());
		byte[] revKeySalt = getRevKeySalt(ciphertext);
		
		if(!Arrays.equals(revKeySalt, tag.keySalt)) throw new SecurityException();
		deserialize(textKey().wrappedDecrypt(ciphertext));
		RevisionTag recreated = new RevisionTag(this, ciphertext);
		if(!Arrays.equals(recreated.getTag(), tag.getTag())) throw new SecurityException();
	}
	
	// write encrypted revision file to storage (path is dictated by revision contents and archive location)
	public void write() throws IOException {
		byte[] ciphertext = textKey().wrappedEncrypt(serialize(), REVISION_FILE_SIZE);
		RevisionTag tag = new RevisionTag(this, ciphertext);
		
		fs.getStorage().write(tag.getPath(), ciphertext);
		fs.getStorage().squash(tag.getPath());
		this.tag = tag;
	}
	
	public void makeActive() throws IOException {
		String revFile = tag.getPath();
		fs.getStorage().mkdirp(Paths.get(ZKFS.ACTIVE_REVISION).getParent().toString());
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
		for(RevisionTag parent : parents) buf.put(parent.getTag());
		
		buf.put(supernodeText);
		
		return buf.array();
	}
	
	// load plaintext serialized revision data  
	private void deserialize(byte[] serialized) {
		this.parents = new ArrayList<RevisionTag>();
		
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		
		int parentTableLen = buf.getInt();
		for(int i = 0; i < parentTableLen/fs.getCrypto().hashLength(); i++) {
			byte[] parent = new byte[fs.getCrypto().hashLength()];
			buf.get(parent);
			parents.add(new RevisionTag(fs, parent));
		}
		
		int supernodeLen = buf.getInt();
		buf.position(buf.position()-4);
		byte[] supernodeText = new byte[supernodeLen+4];
		buf.get(supernodeText);
		
		this.supernode = new Inode(fs, supernodeText);
	}
	
	public byte[] getRevKeySalt(byte[] ciphertext) {
		// TODO: unsuitable, use authenticated hash instead
		return Util.truncateArray(ciphertext, RevisionTag.KEY_SALT_SIZE);
	}
	
	// key used to encrypt revision data
	private Key textKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_CIPHER, ZKFS.KEY_INDEX_REVISION);
	}
	
	// Inode containing info about InodeTable
	public Inode getSupernode() {
		return supernode;
	}
	
	public int getNumParents() {
		return parents.size();
	}
	
	public RevisionTag getParentTag(int index) {
		return parents.get(index);
	}
	
	public RevisionTag getTag() {
		return tag;
	}
}
