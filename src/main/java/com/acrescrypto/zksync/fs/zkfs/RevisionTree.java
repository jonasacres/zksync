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
	protected ArrayList<RefTag> branchTips = new ArrayList<RefTag>();
	protected ZKArchive archive;
	
	public RevisionTree(ZKArchive archive) throws IOException {
		this.archive = archive;
		try {
			read();
		} catch(ENOENTException exc) {
			branchTips = new ArrayList<RefTag>();
		}
	}
	
	public ArrayList<RefTag> branchTips() {
		return branchTips;
	}
	
	public void addBranchTip(RefTag newBranch) {
		branchTips.add(newBranch);
	}
	
	public void removeBranchTip(RefTag oldBranch) {
		branchTips.remove(oldBranch);
	}
	
	public HashSet<RefTag> ancestorsOf(RefTag revision) throws IOException {
		HashSet<RefTag> set = new HashSet<RefTag>();
		addAncestorsOf(revision, set);
		return set;
	}
	
	protected void addAncestorsOf(RefTag revision, HashSet<RefTag> set) throws IOException {
		if(revision == null) return;
		set.add(revision);
		
		Collection<RefTag> parents = revision.getInfo().parents;
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
	
	public void write() throws IOException {
		/* TODO: Part of me wants to hide this in with the rest of the archive data, but I don't see how to make
		 * it work given that, unlike archive data, this file is mutable. If it gets synced across with the real
		 * stuff, one person's is going to overwrite the other's, creating a race condition (and possibly a really
		 * dumb outcome where someone loses the ability to read the branch tip with all their stuff).
		 * 
		 * The files could get keyed to something specific, like an ID in the daemon or something. Then people would
		 * clutter up each other's data directories with their own unique, snowflake copy of the branch list.
		 * 
		 * One possible solution would be to thumbprint them in some way that, although their filenames would reveal
		 * no information to an observer, someone in possession of the archive key could recognize it as a branch
		 * file, with a negligible chance of confusion.
		 * 
		 * One solution might be to derive some unique prefix, and automatically reject and prune any observed files
		 * matching this and not some user-specific suffix.
		 * 
		 * But, if I had two users' data directories, and compared the differences, I'd see the matched prefixes and
		 * be able to distinguish some limited information about the number of branches. This is not catastrophic,
		 * but not ideal. It seems likely to be an issue in any solution, since two peers in sync will differ only
		 * in their branch files.
		 * 
		 * A better solution may be to disguise branch files of all monitored archives together with a random
		 * number (significantly larger than the actual number of branch files) of decoy files. This will make it
		 * difficult to distinguish the number of archives on a system, or the size of the archive sets. A single
		 * file may be hidden amongst them using some hash derived from the user's passphrase, providing an index
		 * of actual meaningful branch files.
		 * 
		 * So really, there are two "balls of wax": an objective one that is synced with peers, and a subjective
		 * one that is never intentionally shared, but can't be considered private.
		 * 
		 * "Wax" might be a cooler name than "zksync." Archives could be called "waxballs."
		 */
		// 64kib branch files seem reasonable
		archive.storage.safeWrite(getPath(), branchTipKey().wrappedEncrypt(serialize(), 1024*64));
	}
	
	protected void read() throws IOException {
		deserialize(branchTipKey().wrappedDecrypt(archive.storage.safeRead(getPath())));
	}
	
	protected void deserialize(byte[] serialized) {
		branchTips.clear();
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		byte[] tag = new byte[archive.crypto.hashLength() + RefTag.REFTAG_EXTRA_DATA_SIZE]; // TODO: this freaking value needs to be a method on archive or something
		while(buf.remaining() >= (archive.crypto.hashLength() + RefTag.REFTAG_EXTRA_DATA_SIZE)) {
			buf.get(tag);
			branchTips.add(new RefTag(archive, tag));
		}
		
		if(buf.hasRemaining()) throw new InvalidArchiveException("branch tip file appears corrupt: " + getPath());
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate((archive.crypto.hashLength() + RefTag.REFTAG_EXTRA_DATA_SIZE)*branchTips.size());
		for(RefTag tag : branchTips) buf.put(tag.getBytes());
		return buf.array();
	}
	
	protected Key branchTipKey() {
		return archive.deriveKey(ZKArchive.KEY_TYPE_CIPHER, ZKArchive.KEY_INDEX_REVISION_TREE);
	}
}
