package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;

/* Stores a revision of the archive. This is needed to bootstrap reading the archive.
 */
public class RevisionInfo extends ZKFile {
	HashSet<RevisionTag> parents = new HashSet<>();
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
		this(fs.inodeTable.inodeWithId(InodeTable.INODE_ID_REVISION_INFO));
	}
	
	public RevisionInfo(Inode inode) throws IOException {
		super(inode.fs);
		this.path = REVISION_INFO_PATH;
		this.mode = O_RDWR;
		this.inode = inode;
		this.tree = new PageTree(this.inode);
		load();
	}

	public void load() throws IOException {
		rewind();
		deserialize(read());
	}
	
	public void addParent(RevisionTag parent) throws IOException {
		generation = Math.max(generation, 1+parent.height);
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
	
	// create plaintext serialized revision data (to be encrypted and written to
	// storage)
	protected byte[] serialize() throws IOException {
		addParent(zkfs.baseRevision);
		int len = 8 + 4 + parents.size()*RevisionTag.sizeForConfig(zkfs.archive.config);
		ByteBuffer buf = ByteBuffer.allocate(len);
		buf.putLong(generation);
		buf.putInt(parents.size());
		for(RevisionTag parent : parents) buf.put(parent.getBytes());
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
			byte[] parentBytes = new byte[RevisionTag.sizeForConfig(zkfs.archive.config)];
			buf.get(parentBytes);
			parents.add(new RevisionTag(zkfs.archive.config, parentBytes));
		}
	}

	public long getGeneration() {
		return generation;
	}

	public int getNumParents() {
		return parents.size();
	}
	
	public ArrayList<RevisionTag> getParents() {
		return new ArrayList<>(parents);
	}
}
