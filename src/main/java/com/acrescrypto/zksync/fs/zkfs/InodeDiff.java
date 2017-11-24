package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.exceptions.ENOENTException;

public class InodeDiff {
	HashMap<Inode,ArrayList<RefTag>> resolutions = new HashMap<Inode,ArrayList<RefTag>>();  
	
	public InodeDiff(long inodeId, RefTag[] candidates) throws IOException {
		for(RefTag candidate : candidates) {
			Inode inode = null;
			try {
				inode = candidate.readOnlyFS().inodeTable.inodeWithId(inodeId);
			} catch (ENOENTException e) {}
			resolutions.putIfAbsent(inode, new ArrayList<RefTag>());
			resolutions.get(inode).add(candidate);
		}
	}
	
	public boolean isConflict() {
		return resolutions.size() > 1;
	}
}
