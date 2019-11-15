package com.acrescrypto.zksyncweb.data;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.fs.zkfs.InodeTable;
import com.acrescrypto.zksync.fs.zkfs.RevisionInfo;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

public class XRevisionInfo {
	private byte[] revTag;
	private Long generation;
	private XRevisionInfo[] parents;
	private Long timestamp;
	
	public XRevisionInfo(RevisionTag tag, int depth) throws IOException {
		try(ZKFS fs = tag.getFS()) {
			RevisionInfo info = fs.getRevisionInfo();
			this.setRevTag(tag.getBytes());
			this.setGeneration(info.getGeneration());
			this.timestamp = info.getInodeTable().inodeWithId(InodeTable.INODE_ID_INODE_TABLE).getModifiedTime();
			
			if(depth != 0) { // let negative numbers imply unlimited depth
				this.setParents(new XRevisionInfo[info.getParents().size()]);
				
				for(int i = 0; i < getParents().length; i++) {
					parents[i] = new XRevisionInfo(info.getParents().get(i), depth-1);
				}
			}
		}
	}

	public XRevisionInfo() {
	}

	public Long getGeneration() {
		return generation;
	}

	public void setGeneration(Long generation) {
		this.generation = generation;
	}

	public byte[] getRevTag() {
		return revTag;
	}

	public void setRevTag(byte[] revTag) {
		this.revTag = revTag;
	}

	public XRevisionInfo[] getParents() {
		return parents;
	}

	public void setParents(XRevisionInfo[] parents) {
		this.parents = parents;
	}
	
	public void setTimestamp(Long timestamp) {
		this.timestamp = timestamp;
	}
	
	public Long getTimestamp() {
		return timestamp;
	}
	
	@Override
	public int hashCode() {
		return revTag == null ? 0 : ByteBuffer.wrap(revTag).getInt();
	}
	
	@Override
	public boolean equals(Object o) {
		if(!(o instanceof XRevisionInfo)) return false;
		return Arrays.equals(revTag, ((XRevisionInfo) o).revTag);
	}
}
