package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;

public class DiffSet {
	Revision[] revisions;
	RevisionTag commonAncestor;
	
	private HashMap<RevisionTag,ZKFS> filesystems = new HashMap<RevisionTag,ZKFS>();
	ArrayList<FileDiff> diffs = new ArrayList<FileDiff>();
	
	public DiffSet(Revision[] revisions) throws IOException {
		this.revisions = revisions;
		
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
	
	public ZKFS openFS(RevisionTag tag) throws IOException {
		filesystems.putIfAbsent(tag, new ZKFS(new Revision(tag)));
		return filesystems.get(tag);
	}
	
	public HashSet<String> allPaths() throws IOException {
		HashSet<String> allPaths = new HashSet<String>();
		// allPaths.add("/");
		
		for(Revision rev : revisions) {
			ZKFS fs = openFS(rev.tag);
			for(String path : fs.opendir("/").listRecursive()) {
				allPaths.add(path);
			}
		}
		return allPaths;
	}
	
	public FileDiff versionsOfFile(String path) throws IOException {
		FileDiff diff = new FileDiff(path);
		for(Revision rev : revisions) {
			diff.addVersion(rev, versionOfFileForTag(rev.tag, path));
			// TODO: this would be a great place to dump / and figure out how we're getting all these diffs
		}
		return diff;
	}
	
	public boolean isResolved() {
		return getUnresolvedDiffs().isEmpty();
	}
	
	protected Inode versionOfFileForTag(RevisionTag tag, String path) throws IOException {
		try {
			return openFS(tag).inodeForPath(path);
		} catch (ENOENTException e) {
			return null;
		}
	}
	
	public Revision applyResolution() throws IOException {
		ZKFS fs = openFS(revisions[0].tag);
		
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
