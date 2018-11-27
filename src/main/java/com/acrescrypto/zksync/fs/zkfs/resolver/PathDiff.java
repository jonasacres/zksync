package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;

public class PathDiff implements Comparable<PathDiff> {
	protected HashMap<Long,ArrayList<RevisionTag>> resolutions = new HashMap<Long,ArrayList<RevisionTag>>();
	protected String path;
	protected boolean resolved;
	protected Long resolution;
	
	public PathDiff(String path) {
		this.path = path;
	}
	
	public PathDiff(String path, RevisionTag[] candidates, Map<Long, Map<RevisionTag, Long>> idMap) throws IOException {
		this.path = path;
		for(RevisionTag candidate : candidates) {
			Long inodeId = null;
			try {
				inodeId = candidate.readOnlyFS().inodeForPath(path).getStat().getInodeId();
				if(idMap != null && idMap.containsKey(inodeId)) inodeId = idMap.get(inodeId).getOrDefault(candidate, inodeId);
			} catch (ENOENTException e) {}
			getResolutions().putIfAbsent(inodeId, new ArrayList<>());
			getResolutions().get(inodeId).add(candidate);
		}
	}

	public boolean isConflict() {
		return getResolutions().size() > 1;
	}
	
	public String getPath() {
		return path;
	}

	public HashMap<Long,ArrayList<RevisionTag>> getResolutions() {
		return resolutions;
	}

	public boolean isResolved() {
		return resolved;
	}
	
	public void setResolution(Long resolution) {
		this.resolution = resolution;
		this.resolved = true;
	}

	@Override
	public int compareTo(PathDiff other) {
		/* we want nulls (path deletions) to go at the end, and short paths to come before long paths.
		   this makes sure we handle moves without unlinking the file before relinking it, and that
		   directories are created before children (whose paths must be longer).
		 */
		if(resolution == null) {
			if(other.resolution != null) {
				return 1;
			}
		} else if(other.resolution == null) {
			return -1;
		}
		
		return path.compareTo(other.path);
	}

	public String toString() {
		return "PathDiff " + path + " (" + resolutions.size() + " versions)";
	}
}
