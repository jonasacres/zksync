package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import com.acrescrypto.zksync.exceptions.TooManyParentsException;

/* Stores a revision of the archive. This is needed to bootstrap reading the archive.
 */
public class RevisionInfo {
	public final static int FIXED_SIZE = 2048; // serialized length of RevisionInfo, regardless of contents
	public final static int USABLE_SIZE = FIXED_SIZE-8-4;
	
	ArrayList<RevisionTag> parents = new ArrayList<>();
	long generation; // longest path to this revision from root
	InodeTable inodeTable;
	
	public static String REVISION_INFO_PATH = "(revision info)";
	
	public static int maxParentsForConfig(ZKArchiveConfig config) {
		return USABLE_SIZE/RevisionTag.sizeForConfig(config);
	}
	
	public RevisionInfo(InodeTable inodeTable, Collection<RevisionTag> parents, long generation) throws TooManyParentsException {
		this.inodeTable = inodeTable;
		this.parents = new ArrayList<>(parents);
		this.generation = generation;
		assert(generation >= 0);
		if(parents.size() > maxParentsForConfig(inodeTable.zkfs.archive.config)) {
			int i = 0;
			for(RevisionTag parent : parents) {
				System.out.println((++i) + ": " + parent);
			}
			System.out.println(parents.size() + " exceeds " + maxParentsForConfig(inodeTable.zkfs.archive.config));
			throw new TooManyParentsException();
		}
	}
	
	public RevisionInfo(InodeTable inodeTable, byte[] serialized) throws IOException {
		this.inodeTable = inodeTable;
		deserialize(serialized);
	}
	
	public void reset() {
		parents.clear();
	}
	
	// create plaintext serialized revision data (to be encrypted and written to
	// storage)
	protected byte[] serialize() throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(FIXED_SIZE);
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
		
		int revTagSize = RevisionTag.sizeForConfig(inodeTable.zkfs.archive.config);
		for(int i = 0; i < numParents; i++) {
			byte[] parentBytes = new byte[revTagSize];
			buf.get(parentBytes);
			parents.add(new RevisionTag(inodeTable.zkfs.archive.config, parentBytes));
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
