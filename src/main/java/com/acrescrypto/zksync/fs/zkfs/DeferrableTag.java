package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Arrays;

import com.acrescrypto.zksync.utility.Util;

public class DeferrableTag {
	protected ZKArchive archive;
	protected Block block;
	protected DeferrableRefTag deferrableRefTag;
	protected byte[] bytes;
	protected boolean isBlank, isImmediate;
	protected long numSerializablePages = 1;
	
	/* blank, withBytes and withImmediate do the same thing right now, but it "felt"
	 * useful to keep semantic separation between them... */
	
	/** Initialize a blank tag, indicating no data is stored at a location */
	public static DeferrableTag blank(ZKArchive archive) {
		byte[] blankBytes = new byte[archive.getCrypto().hashLength()];
		return new DeferrableTag(archive, blankBytes);
	}
	
	/** Initialize a tag from raw serialized bytes containing a tag */
	public static DeferrableTag withBytes(ZKArchive archive, byte[] bytes) {
		return new DeferrableTag(archive, bytes);
	}
	
	/** Initialize a tag from raw serialized bytes containing an immediate value */
	public static DeferrableTag withImmediate(ZKArchive archive, byte[] bytes) {
		return new DeferrableTag(archive, bytes);
	}

	public class DeferrableRefTag extends RefTag {
		@Override
		public byte[] getBytes() {
			ensureFinalized();
			return super.getBytes();
		}
		
		@Override
		public byte[] getHash() {
			ensureFinalized();
			return super.getHash();
		}
		
		@Override
		public long getNumPages() {
			ensureFinalized();
			return super.getNumPages();
		}
		
		@Override
		public int getRefType() {
			ensureFinalized();
			return super.getRefType();
		}
		
		@Override
		public boolean isBlank() {
			return false;
		}
		
		@Override
		public byte[] getLiteral() {
			ensureFinalized();
			return super.getLiteral();
		}
		
		@Override
		public boolean equals(Object other) {
			ensureFinalized();
			return super.equals(other);
		}
		
		@Override
		public int compareTo(RefTag other) {
			ensureFinalized();
			return super.compareTo(other);
		}
		
		@Override
		public String toString() {
			if(block.isWritable()) {
				return "(Unfinalized DeferrableRefTag)";
			}
			
			return super.toString();
		}

		
		public void ensureFinalized() {
			if(tag == null) {
				makeRefTag();
			}
		}
		
		public void makeRefTag() {
			this.config = block.archive.config;
			this.hash = padHash(getBytes());
			this.numPages = numSerializablePages;
			
			if(bytes != null) {
				makeImmediateRefTag();
			} else if(numPages == 1) {
				makeIndirectRefTag();
			} else {
				make2IndirectRefTag();
			}
		}
		
		public void makeImmediateRefTag() {
			this.refType = RefTag.REF_TYPE_IMMEDIATE;
			this.tag = bytes;
		}

		public void makeIndirectRefTag() {
			this.refType = RefTag.REF_TYPE_INDIRECT;
			this.tag = serialize();
		}

	
		public void make2IndirectRefTag() {
			this.refType = RefTag.REF_TYPE_2INDIRECT;
			this.tag = serialize();
		}
	}
	
	public DeferrableTag(Block block) {
		this.archive = block.archive;
		this.block = block;
		this.deferrableRefTag = new DeferrableRefTag();
	}
	
	public DeferrableTag(ZKArchive archive, byte[] bytes) {
		this.bytes = bytes;
		this.deferrableRefTag = new DeferrableRefTag();
		
		if(bytes.length < archive.crypto.hashLength()) {
			isImmediate = true;
		} else if(Arrays.equals(bytes, new byte[archive.crypto.hashLength()])) {
			isBlank = true;
		}
	}
	
	public Block getBlock() throws IOException {
		if(block == null) {
			block = new Block(this);
		}
		
		return block;
	}
	
	public byte[] getBytes() throws IOException {
		if(bytes != null) {
			return bytes;
		}
		
		synchronized(this) {
			if(block.isWritable()) {
				block.write();
			}
		}
		
		return bytes;
	}
	
	public DeferrableRefTag getRefTag() {
		return deferrableRefTag;
	}
	
	public boolean isImmediate() {
		return bytes != null;
	}
	
	public boolean isBlank() {
		return isBlank;
	}
	
	public boolean isPending() {
		return bytes == null;
	}

	public String path() throws IOException {
		return Page.pathForTag(bytes);
	}
	
	public void setNumPages(long numPages) {
		this.numSerializablePages = numPages;
	}
	
	@Override
	public String toString() {
		if(bytes != null) {
			return String.format("Immediate (%d): %s%s",
					bytes.length,
					Util.bytesToHex(bytes, 6),
					bytes.length > 6 ? "..." : "");
		}
		
		if(block.isWritable()) {
			return "(Unfinalized DeferredTag)";
		}
		
		try {
			return Util.formatPageTag(getBytes());
		} catch (IOException exc) {
			// shouldn't be possible since if we're not writable we should be serialized already
			exc.printStackTrace();
			return "(ERROR)";
		}
	}
	
	@Override
	public boolean equals(Object other) {
		if(other == this) return true;
		if(!(other instanceof DeferrableTag)) return false;
		DeferrableTag o = (DeferrableTag) other;
		
		return block.equals(o.block);
	}

	protected void setBytes(byte[] bytes) {
		this.bytes = bytes;
	}
}
