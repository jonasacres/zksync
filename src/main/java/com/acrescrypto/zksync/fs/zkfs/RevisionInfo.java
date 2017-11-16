package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;

/* Stores a revision of the archive. This is needed to bootstrap reading the archive.
 */
public class RevisionInfo extends ZKFile {
	HashSet<RefTag> parents = new HashSet<RefTag>();
	long generation; // longest path to this revision from root
	
	public static String REVISION_INFO_PATH = "(revision info)";
	
	/* TODO No-auto-merge flag
	 * Right now, it looks like in most use cases, branches are unintentional and undesirable. So, I'm making a pareto
	 * assumption that it should be as easy as possible to merge branches quickly and automatically. However, there
	 * are probably some cases in which branching is desired.
	 * 
	 * It'd be nice to have a "NO_AUTO_MERGE" flag in a RevisionInfo that says "don't automatically merge me or any
	 * of my descendants with my siblings or any of their descendants." People can manually merge as desired from
	 * there.
	 */
	
	public RevisionInfo(ZKFS fs) throws IOException {
		this.fs = fs;
		this.path = REVISION_INFO_PATH;
		this.mode = O_RDWR;
		this.inode = fs.inodeTable.inodeWithId(InodeTable.INODE_ID_REVISION_INFO);
		this.merkel = new PageMerkel(this.inode.getRefTag());
		load();
	}

	public void load() throws IOException {
		rewind();
		deserialize(read());
	}
	
	public void addParent(RefTag parent) throws IOException {
		generation = Math.max(generation, 1+parent.getInfo().generation);
		parents.add(parent);
	}
	
	public void reset() {
		parents.clear();
	}
	
	public void commit() throws IOException {
		rewind();
		truncate(0);
		write(serialize());
		flush();
	}
	
	protected int parentTagLength() {
		return fs.archive.crypto.hashLength()+RefTag.REFTAG_EXTRA_DATA_SIZE;
	}

	// create plaintext serialized revision data (to be encrypted and written to
	// storage)
	protected byte[] serialize() throws IOException {
		addParent(fs.baseRevision);
		int len = 8 + 4 + parents.size()*parentTagLength();
		ByteBuffer buf = ByteBuffer.allocate(len);
		buf.putLong(generation);
		buf.putInt(parents.size());
		for(RefTag parent : parents) buf.put(parent.getBytes());
		return buf.array();
	}

	// load plaintext serialized revision data
	protected void deserialize(byte[] serialized) {
		parents.clear();
		if(serialized.length == 0) {
			this.generation = 0;
			return;
		}
		
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.generation = buf.getLong();
		
		int numParents = buf.getInt();
		for(int i = 0; i < numParents; i++) {
			byte[] parentBytes = new byte[parentTagLength()];
			buf.get(parentBytes);
			parents.add(new RefTag(fs.archive, parentBytes));
		}
	}

	public long getGeneration() {
		return generation;
	}

	public int getNumParents() {
		return parents.size();
	}
	
	public ArrayList<RefTag> getParents() {
		return new ArrayList<RefTag>(parents);
	}
}
