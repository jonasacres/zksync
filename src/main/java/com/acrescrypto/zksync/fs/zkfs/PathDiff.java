package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.ENOENTException;

public class PathDiff {
	HashMap<Long,ArrayList<RefTag>> resolutions = new HashMap<Long,ArrayList<RefTag>>();  
	
	public PathDiff(String path, RefTag[] candidates) throws IOException {
		for(RefTag candidate : candidates) {
			Long inodeId = null;
			try {
				inodeId = candidate.readOnlyFS().inodeForPath(path).stat.getInodeId();
			} catch (ENOENTException e) {}
			resolutions.putIfAbsent(inodeId, new ArrayList<RefTag>());
			resolutions.get(inodeId).add(candidate);
		}
	}

	public boolean isConflict() {
		return resolutions.size() > 1;
	}
}
