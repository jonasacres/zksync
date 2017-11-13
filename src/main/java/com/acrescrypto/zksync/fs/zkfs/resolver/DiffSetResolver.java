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
	
	DiffSet diffSet;
	ZKFS fs;
	FileDiffResolver fileResolver;
	
	HashMap<String,ArrayList<FileDiff>> diffsByDir = new HashMap<String,ArrayList<FileDiff>>();
	
	public DiffSetResolver(DiffSet diffSet, FileDiffResolver fileResolver) throws IOException {
		this.diffSet = diffSet;
		this.fileResolver = fileResolver;
		this.fs = new ZKFS(diffSet.latestRevision());
	}
	
	public RefTag resolve() throws IOException, DiffResolutionException {
		resolveNonDirectories();
		resolveDirectories();
		return fs.commit(diffSet.getRevisions(), null); // TODO: need to derive some secure seed here
	}
	
	protected void resolveNonDirectories() throws IOException {
		for(FileDiff diff : diffSet.getDiffs()) {
			String dirname = fs.dirname(diff.getPath());
			diffsByDir.putIfAbsent(dirname, new ArrayList<FileDiff>());
			diffsByDir.get(dirname).add(diff);

			if(diff.isResolved()) continue;
			diff.resolve(fileResolver.resolve(this, diff));
		}
	}
	
	protected void resolveDirectories() throws IOException, DiffResolutionException {
		for(String alteredDirectory : diffsByDir.keySet()) {
			FileDiff diff = diffSet.diffForPath(alteredDirectory);
			if(diff == null) throw new IllegalStateException("altered directory " + alteredDirectory + " does not appear in diffset");
			if(!diff.isResolved()) throw new UnresolvedDiffException(diff);
			if(!diff.getResolution().getStat().isDirectory()) throw new InconsistentDiffResolutionException(diff);
			
			ZKDirectory dir = fs.opendir(alteredDirectory);
			for(FileDiff fileDiff : diffsByDir.get(alteredDirectory)) {
				String basename = fs.basename(fileDiff.getPath());
				if(!fileDiff.isResolved()) throw new UnresolvedDiffException(fileDiff);
				if(fileDiff.getResolution() == null) {
					if(dir.contains(basename)) dir.unlink(basename);
				} else {
					if(!dir.contains(basename)) dir.link(fileDiff.getResolution(), basename);
				}
			}
		}
	}
	
	public DiffSet getDiffSet() {
		return diffSet;
	}
	
	public ZKFS getFs() {
		return fs;
	}
}
