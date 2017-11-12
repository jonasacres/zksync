package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;

public class RevisionTree {
	protected ArrayList<RefTag> branchTips;
	protected ZKArchive archive;
	protected int size;
	
	public RevisionTree(ZKArchive archive) throws IOException {
		this.archive = archive;
		try {
			read();
		} catch(ENOENTException exc) {
			branchTips = new ArrayList<RefTag>();
		}
	}
	
	public int size() {
		return size;
	}
	
	public ArrayList<RefTag> branchTips() {
		return branchTips;
	}
	
	public HashSet<RefTag> ancestorsOf(RefTag revision) throws IOException {
		HashSet<RefTag> set = new HashSet<RefTag>();
		addAncestorsOf(revision, set);
		return set;
	}
	
	protected void addAncestorsOf(RefTag revision, HashSet<RefTag> set) throws IOException {
		if(revision == null) return;
		set.add(revision);
		
		ArrayList<RefTag> parents = revision.getInfo().parents; // TODO: make sure that this doesn't end up decrypting the revisioninfo twice
		for(RefTag parent : parents) {
			addAncestorsOf(parent, set);
		}
	}
	
	public RefTag commonAncestorOf(RefTag[] revisions) throws IOException {
		HashSet<RefTag> allAncestors = null;
		for(RefTag rev : revisions) {
			Collection<RefTag> ancestors = ancestorsOf(rev);
			
			if(allAncestors == null) allAncestors = new HashSet<RefTag>(ancestors);
			else allAncestors.retainAll(ancestors);
		}
		
		RefTag youngest = null;
		for(RefTag tag : allAncestors) {
			if(youngest == null || tag.getInfo().generation > youngest.getInfo().generation) youngest = tag;
		}
		return youngest;
	}
	
	public String getPath() {
		return Paths.get(ZKArchive.REVISION_DIR, "branch-tips").toString();
	}
	
	protected void write() throws IOException {
		// 64kib branch files seem reasonable
		// TODO: what happens when we bust the limit? that's 1024 branch tips, which is a lot, but it could happen
		archive.storage.write(getPath(), branchTipKey().wrappedEncrypt(serialize(), 1024*64));
	}
	
	protected void read() throws IOException {
		deserialize(branchTipKey().wrappedDecrypt(archive.storage.read(getPath())));
	}
	
	protected void deserialize(byte[] serialized) {
		branchTips.clear();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] tag = new byte[archive.crypto.hashLength()];
		while(buf.remaining() > archive.crypto.hashLength()) {
			buf.get(tag);
			branchTips.add(new RefTag(archive, tag));
		}
		
		if(buf.hasRemaining()) throw new InvalidArchiveException("branch tip file appears corrupt: " + getPath());
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(archive.crypto.hashLength()*branchTips.size());
		for(RefTag tag : branchTips) buf.put(tag.getBytes());
		return buf.array();
	}
	
	protected Key branchTipKey() {
		return archive.deriveKey(ZKArchive.KEY_TYPE_CIPHER, ZKArchive.KEY_INDEX_REVISION_TREE);
	}
}
