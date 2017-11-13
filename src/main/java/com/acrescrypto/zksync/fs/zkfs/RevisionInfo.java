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
	
	public RevisionInfo(ZKFS fs) throws IOException {
		this.fs = fs;
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

	public int getNumParents() {
		return parents.size();
	}
	
	public ArrayList<RefTag> getParents() {
		return new ArrayList<RefTag>(parents);
	}
}
