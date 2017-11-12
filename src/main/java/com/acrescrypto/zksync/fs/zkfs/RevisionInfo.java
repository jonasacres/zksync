package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;

/* Stores a revision of the archive. This is needed to bootstrap reading the archive.
 */
public class RevisionInfo extends ZKFile {
	ArrayList<RefTag> parents;
	long generation; // shortest path to this revision from root

	// make a new revision based on the contents of an inode table
	public RevisionInfo(ZKFS fs) {
		this.generation = 1+fs.inodeTable.revision.generation;
		this.fs = fs;
		
		addParent(fs.currentRefTag());
	}
	
	public void addParent(RefTag parent) {
		generation = Math.min(generation, 1+parent.getInfo().generation);
		parents.add(parent);
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
	protected byte[] serialize() {
		// TODO: not a fan of how brittle our size calculation looks... but it can't be a constant, either.
		int len = 4 + parents.size()*parentTagLength();
		ByteBuffer buf = ByteBuffer.allocate(len);
		buf.putLong(generation);
		buf.putInt(parents.size());
		for(RefTag parent : parents) buf.put(parent.getBytes());
		return buf.array();
	}

	// load plaintext serialized revision data
	protected void deserialize(byte[] serialized) {
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.generation = buf.getLong();
		
		int authorLen = buf.getShort();
		byte[] authorBytes = new byte[authorLen];
		buf.get(authorBytes);
		
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
}
