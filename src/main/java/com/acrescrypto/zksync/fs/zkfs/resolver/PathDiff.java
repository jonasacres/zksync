package com.acrescrypto.zksync.fs.zkfs.resolver;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.zkfs.RefTag;

public class PathDiff implements Comparable<PathDiff> {
	protected HashMap<Long,ArrayList<RefTag>> resolutions = new HashMap<Long,ArrayList<RefTag>>();
	protected String path;
	protected boolean resolved;
	protected Long resolution;
	
	public PathDiff(String path, RefTag[] candidates) throws IOException {
		this.path = path;
		for(RefTag candidate : candidates) {
			Long inodeId = null;
			try {
				inodeId = candidate.readOnlyFS().inodeForPath(path).getStat().getInodeId();
			} catch (ENOENTException e) {}
			getResolutions().putIfAbsent(inodeId, new ArrayList<RefTag>());
			getResolutions().get(inodeId).add(candidate);
		}
	}

	public boolean isConflict() {
		return getResolutions().size() > 1;
	}
	
	public String getPath() {
		return path;
	}

	public HashMap<Long,ArrayList<RefTag>> getResolutions() {
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
		return path.compareTo(other.path);
	}
}
