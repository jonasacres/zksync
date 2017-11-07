package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.Directory;

public class DiffSet {
	Revision[] revisions;
	RevisionTag commonAncestor;
	
	private HashMap<RevisionTag,ZKFS> filesystems;
	ArrayList<FileDiff> diffs;
	
	public DiffSet(Revision[] revisions) throws IOException {
		this.revisions = revisions;
		openFilesystems();
		
		RevisionTag[] tags = new RevisionTag[revisions.length];
		for(int i = 0; i < revisions.length; i++) tags[i] = revisions[i].tag;
		commonAncestor = revisions[0].fs.getRevisionTree().commonAncestorOf(tags);
		
		for(String path : allPaths()) {
			FileDiff diff = versionsOfFile(path);
			if(diff.hasMultipleVersions()) {
				diffs.add(diff);
			}
		}
		
		filesystems = null;
	}
	
	public ArrayList<FileDiff> getDiffs() {
		return diffs;
	}
	
	public ArrayList<FileDiff> getUnresolvedDiffs() {
		ArrayList<FileDiff> unresolved = new ArrayList<FileDiff>();
		for(FileDiff diff : diffs) {
			if(diff.getResolution() == null) continue;
			unresolved.add(diff);
		}
		
		return unresolved;
	}
	
	public void openFilesystems() throws IOException {
		filesystems = new HashMap<RevisionTag,ZKFS>();
		for(Revision rev : revisions) {
			filesystems.put(rev.tag, new ZKFS(rev));
		}
	}
	
	public HashSet<String> allPaths() throws IOException {
		HashSet<String> allPaths = new HashSet<String>();
		for(ZKFS fs : filesystems.values()) {
			for(String path : fs.opendir("/").listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES)) {
				allPaths.add(path);
			}
		}
		return allPaths;
	}
	
	public FileDiff versionsOfFile(String path) throws IOException {
		FileDiff diff = new FileDiff(path);
		for(Revision rev : revisions) {
			Inode original = filesystems.get(commonAncestor).inodeForPath(path),
				  modified = filesystems.get(rev.tag).inodeForPath(path);
			if(original == null && modified == null) continue;
			else if(original == null || modified == null || original.equals(modified)) diff.addVersion(rev, modified);
		}
		return diff;
	}
	
	public boolean isResolved() {
		return getUnresolvedDiffs().isEmpty();
	}
	
	public Revision applyResolution() throws IOException {
		ZKFS fs = filesystems.get(revisions[0].tag);
		
		for(FileDiff diff : diffs) {
			if(!diff.isResolved()) throw new EINVALException("Unresolved conflicts");
			Inode resolution = diff.resolution;
			if(resolution == null) {
				fs.unlink(diff.path);
			} else {
				fs.inodeTable.replaceInode(resolution.getStat().getInodeId(), resolution);
				ZKDirectory dir = fs.opendir(fs.dirname(diff.path));
				dir.link(resolution, fs.basename(diff.path));
				dir.close();
			}
		}
		
		return fs.commit(revisions, fs.deriveKey(ZKFS.KEY_TYPE_PRNG, ZKFS.KEY_INDEX_REVISION).authenticate(fs.inodeTable.inode.refId));
	}
}
