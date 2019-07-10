package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.utility.Util;

public class PathDiff implements Comparable<PathDiff> {
	protected HashMap<Long,ArrayList<RevisionTag>> resolutions = new HashMap<Long,ArrayList<RevisionTag>>();
	protected String path;
	protected boolean resolved;
	protected Long resolution;
	protected boolean forceConflict;
	
	public PathDiff(String path) {
		this.path = path;
	}
	
	public PathDiff(String path, RevisionTag[] candidates, Map<Long, Map<RevisionTag, Long>> idMap) throws IOException {
		this.path = path;
		for(RevisionTag candidate : candidates) {
			Long inodeId = null;
			try(ZKFS fs = candidate.readOnlyFS()) {
				inodeId = fs.inodeForPath(path, false).getStat().getInodeId();
				if(idMap != null && idMap.containsKey(inodeId)) {
					long newInodeId = idMap.get(inodeId).getOrDefault(candidate, inodeId);
					if(inodeId != newInodeId) {
						Util.debugLog(String.format("PathDiff %s: candidate %s, remapping inodeId for path %s %d -> %d due to diff renumbering",
								fs.getArchive().getMaster().getName(),
								Util.formatRevisionTag(candidate),
								path,
								inodeId,
								newInodeId));
						inodeId = newInodeId;
						
						// need this to appear as a path diff to guarantee renumbering is executed, so set forceConflict
						forceConflict = true;
					}
				}
			} catch (ENOENTException e) {}
			
			getResolutions().putIfAbsent(inodeId, new ArrayList<>());
			getResolutions().get(inodeId).add(candidate);
		}
	}

	public boolean isConflict() {
		return forceConflict || getResolutions().size() > 1;
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
	
	public void clearResolution() {
		this.resolved = false;
		this.resolution = null;
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
