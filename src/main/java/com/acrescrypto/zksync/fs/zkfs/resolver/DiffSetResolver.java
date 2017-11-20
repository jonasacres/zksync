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
		resolveInodes();
		resolvePaths();
		resolveDirectories();
	}
	
	protected void resolveInodes() throws IOException {
		// pick the latest inode
	}
	
	protected void resolvePaths() throws IOException {
		// pick the latest non-null path (or null if this is the only option)
	}
	
	protected void resolveDirectories() throws IOException, DiffResolutionException {
		// ensure that all selected paths are linked, and all nulled paths are unlinked 
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
