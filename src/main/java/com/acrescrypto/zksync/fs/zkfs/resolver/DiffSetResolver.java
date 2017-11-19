package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.DiffResolutionException;
import com.acrescrypto.zksync.exceptions.InconsistentDiffResolutionException;
import com.acrescrypto.zksync.exceptions.UnresolvedDiffException;
import com.acrescrypto.zksync.fs.zkfs.DiffSet;
import com.acrescrypto.zksync.fs.zkfs.FileDiff;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

public class DiffSetResolver {
	public interface FileDiffResolver {
		public Inode resolve(DiffSetResolver setResolver, FileDiff diff);
	}
	
	DiffSet diffset;
	ZKFS fs;
	FileDiffResolver fileResolver;
	
	HashMap<String,ArrayList<FileDiff>> diffsByDir = new HashMap<String,ArrayList<FileDiff>>();
	
	public static DiffSetResolver defaultResolver(DiffSet diffset) throws IOException {
		return latestVersionResolver(diffset);
	}
	
	public static DiffSetResolver latestVersionResolver(DiffSet diffset) throws IOException {
		return new DiffSetResolver(diffset,
				(DiffSetResolver setResolver, FileDiff diff) ->
		{
			return diff.latestVersion();
		});
	}
	
	public DiffSetResolver(DiffSet diffset, FileDiffResolver fileResolver) throws IOException {
		this.diffset = diffset;
		this.fileResolver = fileResolver;
		this.fs = new ZKFS(diffset.latestRevision());
	}
	
	public RefTag resolve() throws IOException, DiffResolutionException {
		resolveNonDirectories();
		assertResolved();
		resolveDirectories();
		for(FileDiff diff : diffset.getDiffs()) {
			fs.getInodeTable().replaceInode(diff.getResolution());
		}
		return fs.commit(diffset.getRevisions(), null); // TODO: need to derive some secure seed here
	}
	
	protected void resolveNonDirectories() throws IOException {
		for(FileDiff diff : diffset.getDiffs()) {
			String dirname = fs.dirname(diff.getPath());
			diffsByDir.putIfAbsent(dirname, new ArrayList<FileDiff>());
			diffsByDir.get(dirname).add(diff);

			if(diff.isResolved()) continue;
			// TODO: what if this is a directory?
			// TODO: what if some of the branches have this as a directory, and some have it as something else?
			/* Directories are complicated because someone might change metadata (uid, gid, mode, etc.) in one
			 * branch, while editing content (adding and removing files) in multiple branches. The correct merge
			 * might have both changes.
			 * 
			 * resolveDirectories ensures that adds and deletions are consistent with final directory contents.
			 * It's ambiguous on the point of metadata.
			 */
			diff.resolve(fileResolver.resolve(this, diff));
		}
	}
	
	protected void resolveDirectories() throws IOException, DiffResolutionException {
		for(String alteredDirectory : diffsByDir.keySet()) {
			FileDiff diff = diffset.diffForPath(alteredDirectory);
			if(diff == null) throw new IllegalStateException("altered directory " + alteredDirectory + " does not appear in diffset");
			if(!diff.getResolution().getInode().getStat().isDirectory()) throw new InconsistentDiffResolutionException(diff);
			
			ZKDirectory dir = fs.opendir(alteredDirectory);
			for(FileDiff fileDiff : diffsByDir.get(alteredDirectory)) {
				String basename = fs.basename(fileDiff.getPath());
				if(fileDiff.getResolution().getInode() == null) {
					if(dir.contains(basename)) dir.unlink(basename);
				} else {
					if(!dir.contains(basename)) dir.link(fileDiff.getResolution().getInode(), basename);
				}
			}
			
			// TODO: ensure final timestamp is consistent
		}
	}
	
	protected void assertResolved() throws UnresolvedDiffException {
		for(FileDiff diff : diffset.getDiffs()) if(!diff.isResolved()) throw new UnresolvedDiffException(diff);
	}
	
	public DiffSet getDiffSet() {
		return diffset;
	}
	
	public ZKFS getFs() {
		return fs;
	}
}
