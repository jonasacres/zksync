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
	public final static int MAX_TITLE_LEN = 256;
	public final static int USABLE_PARENT_SIZE = FIXED_SIZE - MAX_TITLE_LEN - 8 - 4;
	
	protected ArrayList<RevisionTag> parents = new ArrayList<>();
	protected long generation; // longest path to this revision from root
	protected InodeTable inodeTable;
	protected String title;
	
	public static String REVISION_INFO_PATH = "(revision info)";
	
	public static int maxParentsForConfig(ZKArchiveConfig config) {
		return USABLE_PARENT_SIZE/RevisionTag.sizeForConfig(config);
	}
	
	public RevisionInfo(InodeTable inodeTable, Collection<RevisionTag> parents, long generation, String title) throws TooManyParentsException {
		this.inodeTable = inodeTable;
		this.parents = new ArrayList<>(parents);
		this.generation = generation;
		
		if(title.length() <= MAX_TITLE_LEN) {
			this.title = title;
		} else {
			this.title = title.substring(0, MAX_TITLE_LEN);
		}
		
		assert(generation >= 0);
		if(parents.size() > maxParentsForConfig(inodeTable.zkfs.archive.config)) {
			throw new TooManyParentsException();
		}
	}
	
	public RevisionInfo(InodeTable inodeTable, byte[] serialized) throws IOException {
		this.inodeTable = inodeTable;
		deserialize(serialized);
	}
	
	// create plaintext serialized revision data (to be encrypted and written to
	// storage)
	protected byte[] serialize() throws IOException {
		ByteBuffer titleBuf = ByteBuffer.allocate(MAX_TITLE_LEN);
		titleBuf.put(title.getBytes());
		
		ByteBuffer buf = ByteBuffer.allocate(FIXED_SIZE);
		buf.putLong(generation);
		buf.put(titleBuf.array());
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
		
		byte[] titleBytes = new byte[MAX_TITLE_LEN];
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		this.generation = buf.getLong();
		buf.get(titleBytes);
		int numParents = buf.getInt();
		
		this.title = new String(titleBytes);
		
		int revTagSize = RevisionTag.sizeForConfig(inodeTable.zkfs.archive.config);
		byte[] parentBytes = new byte[revTagSize];
		for(int i = 0; i < numParents; i++) {
			buf.get(parentBytes);
			parents.add(new RevisionTag(inodeTable.zkfs.archive.config, parentBytes, false));
		}
	}

	public long getGeneration() {
		return generation;
	}

	public int getNumParents() {
		return parents.size();
	}
	
	public String getTitle() {
		return title;
	}
	
	public ArrayList<RevisionTag> getParents() {
		return new ArrayList<>(parents);
	}
}
