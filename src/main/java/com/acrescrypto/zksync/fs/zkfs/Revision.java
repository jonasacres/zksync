package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PRNG;

/* Stores a revision of the archive. This is needed to bootstrap reading the archive.
 */
public class Revision {
	protected ArrayList<RevisionTag> parents = new ArrayList<RevisionTag>(); // revtags of parent revisions
	protected Inode supernode; // inode for inode table
	protected ZKFS fs; // ZKFS archive to which this revision belongs
	protected RevisionTag tag; // unique, non-confidential identifier for this revision
	long generation;

	/* TODO We have a pretty serious problem here. The revision file size is fixed, to protect privacy.
	 * However, we allow certain variable-sized data, such as the username, group name and (most importantly) the
	 * parent list. Each parent is 64 bytes long. 8 parents is 512 bytes -- enough to fill the current allocation.
	 * 
	 * One approach would be to cap the number of merges. This seems inelegant to me. Although, larger-sized merges
	 * could be done in smaller steps.
	 * 
	 * Another is to write revision history as a file in the zkfs. But then, we can't get the history out without
	 * decrypting and loading the inode table.
	 * 
	 * A compromise might be to store what is possible inside the revision file, and then have the full data in the
	 * inode table. That makes it really hard to calculate leaves, though. Most archives would never experience the
	 * problem, but those that did would have to keep burning time on however many revisions they had that exceeded
	 * the limit, forever (or at least until the revisions were purged from the archive).
	 * 
	 * I do like the idea of a parent record in the archive. It can be encoded as an immediate in most instances,
	 * and it will allow linked-list-like traversal of the revision history even without the revision files. This
	 * would also mean that revision files could be pruned entirely, except for leaves, and then recalculated as
	 * needed.
	 * 
	 * This makes me question whether I have designed revisions poorly. Oh shit. What if the tag IS the revision file,
	 * and given a tag you could begin requesting the inode table? Then the history is un-loseable, even if going
	 * back is O(n). But really -- how often do you want to go that far back?
	 * 
	 * Jesus. Ditching revision files is gonna make me rewrite most of what I've done for the past two weeks.
	 */
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
		generation = Math.max(parentTag.generation + 1, generation);
	}

	// read and decrypt the contents of the revision file at path
	public void read(RevisionTag tag) throws IOException {
		byte[] ciphertext = fs.getStorage().read(tag.getPath());
		byte[] revKeySalt = getRevKeySalt(ciphertext);

		if (!Arrays.equals(revKeySalt, tag.keySalt))
			throw new SecurityException();
		deserialize(textKey().wrappedDecrypt(ciphertext));
		RevisionTag recreated = new RevisionTag(this, ciphertext);
		if (!Arrays.equals(recreated.getTag(), tag.getTag()))
			throw new SecurityException();
		this.tag = tag;
	}

	// write encrypted revision file to storage (path is dictated by revision
	// contents and archive location)
	public void write(byte[] seed) throws IOException {
		PRNG rng = seed == null ? fs.crypto.defaultPrng() : new PRNG(seed);
		byte[] ciphertext = textKey().wrappedEncrypt(serialize(), REVISION_FILE_SIZE, rng);
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

	// create plaintext serialized revision data (to be encrypted and written to
	// storage)
	private byte[] serialize() {
		/*
		 * generation (int64, 8 bytes, number of generations removed we are from root)
		 * parentTableLen (int32, 4 bytes, total size of parent table in bytes) parent
		 * table (parentTableLen/64 entries): parentRevTag (hash, 64 bytes) (Total
		 * record size: 64 bytes) supernodeText (Inode, variable length)
		 */
		byte[] supernodeText = supernode.serialize();
		int parentTableLen = parents.size() * RevisionTag.REV_TAG_SIZE;
		ByteBuffer buf = ByteBuffer.allocate(supernodeText.length + parentTableLen + 12);

		buf.putLong(generation);
		buf.putInt(parentTableLen);
		for (RevisionTag parent : parents)
			buf.put(parent.getTag());

		buf.put(supernodeText);

		return buf.array();
	}

	// load plaintext serialized revision data
	private void deserialize(byte[] serialized) {
		this.parents = new ArrayList<RevisionTag>();

		ByteBuffer buf = ByteBuffer.wrap(serialized);

		this.generation = buf.getLong();
		int parentTableLen = buf.getInt();

		for (int i = 0; i < parentTableLen / RevisionTag.REV_TAG_SIZE; i++) {
			byte[] parent = new byte[RevisionTag.REV_TAG_SIZE];
			buf.get(parent);
			parents.add(new RevisionTag(fs, parent));
		}

		int supernodeLen = buf.getInt();
		buf.position(buf.position() - 4);
		byte[] supernodeText = new byte[supernodeLen + 4];
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

	public long getGeneration() {
		return generation;
	}

	public String toString() {
		return "rev " + Util.bytesToHex(tag.tag);
	}
}
