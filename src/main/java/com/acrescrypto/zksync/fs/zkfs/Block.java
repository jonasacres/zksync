package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.exceptions.BlockDoesNotContainPageException;
import com.acrescrypto.zksync.exceptions.InvalidBlockException;

public class Block {
	protected ZKFS fs;
	protected int remainingCapacity;
	protected byte[] contents;
	protected boolean isWritable;
	protected HashMap<BlockEntryIndex,BlockEntry> entries = new HashMap<>();
	protected DeferrableTag deferrableTag;

	public final static byte INDEX_TYPE_PAGE = 0;
	public final static byte INDEX_TYPE_CHUNK = 1;
	
	public class BlockEntryIndex implements Comparable<BlockEntryIndex> {
		byte type;
		long identity, pageNum;
		
		public BlockEntryIndex(long identity, long pageNum, byte type) {
			this.identity = identity;
			this.pageNum = pageNum;
			this.type = type;
		}
		
		@Override
		public int hashCode() {
			return Long.valueOf(identity ^ type).hashCode();
		}
		
		@Override
		public boolean equals(Object other) {
			if(!(other instanceof BlockEntryIndex)) {
				return false;
			}
			
			BlockEntryIndex o = (BlockEntryIndex) other;
			return o.identity == identity && o.pageNum == pageNum;
		}

		@Override
		public int compareTo(BlockEntryIndex o) {
			int c = Long.valueOf(identity).compareTo(o.identity);
			if(c != 0) {
				return c;
			}
			
			c = Byte.valueOf(type).compareTo(o.type);
			if(c != 0) {
				return c;
			}
			
			return Long.valueOf(pageNum).compareTo(o.pageNum);
		}
	}
	
	public class BlockEntry {
		long identity;
		long pageNum;
		byte type;
		int offset;
		int contentsOffset;
		int length;
		byte[] contents;
		
		public BlockEntry(long identity, long pageNum, byte type) {
			this.identity = identity;
			this.pageNum = pageNum;
			this.type = type;
		}
		
		public BlockEntry(ByteBuffer buf) throws InvalidBlockException {
			this.identity = buf.getLong();
			this.pageNum = buf.getLong();
			long longOffset = buf.getLong();
			
			assertIntegrity(0 <= this.pageNum);
			assertIntegrity(0 < longOffset);
			assertIntegrity(longOffset < fs.getArchive().getConfig().getPageSize());
		}

		public void serialize(ByteBuffer buf) {
			buf.putLong(identity);
			buf.put(type);
			buf.putLong(pageNum);
			buf.putLong(offset);
		}

		public byte[] read() {
			byte[] text = new byte[length];
			System.arraycopy(contents, offset, text, 0, length);
			return text;
		}

		public void setLength(int length) {
			this.length = length;
		}
		
		@Override
		public boolean equals(Object other) {
			if(!(other instanceof BlockEntry)) return false;
			BlockEntry o = (BlockEntry) other;
			if(o == this) return true;
			
			if(o.identity != this.identity) return false;
			if(o.type != this.type) return false;
			if(o.pageNum != this.pageNum) return false;
			if(o.length != this.length) return false;
			for(int i = 0; i < length; i++) {
				if(contents[contentsOffset+i] != o.contents[o.contentsOffset+i]) {
					return false;
				}
			}
			
			return true;
		}
	}
	
	public static int fixedHeaderLength() {
		return 8 + 8; // numEntries, reserved
	}
	
	public static int indexEntryLength() {
		return 8 + 1 + 8 + 8; // identity, type, page, offset
	}
	
	public Block(ZKFS fs) {
		this.fs = fs;
	}
	
	public Block(DeferrableTag tag) throws IOException {
		this.deferrableTag = tag;
		isWritable = false;
		load();
	}
	
	public void addData(long identity, long pageNum, byte type, byte[] contents, int offset, int length) {
		if(!canFitData(contents.length)) {
			throw new RuntimeException("Cannot fit page data into block");
		}
		
		if(contents.length - length >= 1024) {
			/* we don't want to have a bunch of sparsely-written pages in memory, so
			 * resize arrays opportunistically. The original contents array will get
			 * freed when the Page/PageTreeChunk object owning it is evicted.
			 */
			byte[] newContents = new byte[length];
			System.arraycopy(contents, offset, newContents, 0, length);
			contents = newContents;
			offset = 0;
		}
		
		BlockEntryIndex index = new BlockEntryIndex(identity, pageNum, type);
		BlockEntry entry = entries.get(index);
		if(entry == null) {
			entry = new BlockEntry(identity, pageNum, type);
			entries.put(index, entry);
		}
		
		entry.contents = contents;
		entry.contentsOffset = offset;
		entry.length = length;
		remainingCapacity -= length - indexEntryLength();
	}
	
	public boolean removeData(long identity, long chunkNum, byte type) {
		BlockEntryIndex index = new BlockEntryIndex(identity, chunkNum, type);
		return entries.remove(index) != null;
	}

	public boolean canFitData(long length) {
		return remainingCapacity >= length + indexEntryLength();
	}
	
	public byte[] readData(long identity, long pageNum, byte type) throws IOException {
		BlockEntry entry = entries.get(new BlockEntryIndex(identity, pageNum, type));
		if(entry == null) {
			throw new BlockDoesNotContainPageException(identity, pageNum);
		}
		
		return entry.read();
	}
	
	public byte[] write() throws IOException {
		byte[] tagBytes = SignedSecureFile.withParams(fs.getArchive().getStorage(),
			textKey(),
			saltKey(),
			authKey(),
			fs.getArchive().getConfig().getPrivKey())
		.write(serialize(), fs.getArchive().getConfig().getPageSize());
		isWritable = false;
		fs.archive.addPageTag(tagBytes);
		return tagBytes;
	}
	
	public boolean isWritable() {
		return isWritable;
	}
	
	public DeferrableTag getDeferrableTag() {
		return deferrableTag;
	}
	
	protected void load() throws IOException {
		contents = SignedSecureFile.withTag(deferrableTag.getBytes(),
				fs.getArchive().getStorage(),
				textKey(),
				saltKey(),
				authKey(),
				fs.getArchive().getConfig().getPubKey()).read();
		deserialize(contents);
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(fs.getArchive().getConfig().getPageSize());
		buf.putLong(entries.size());
		buf.putLong(0); // reserved
		LinkedList<BlockEntryIndex> indices = new LinkedList<>(entries.keySet());
		indices.sort(null);
		int nextOffset = fixedHeaderLength() + indexEntryLength()*entries.size();
		
		for(BlockEntryIndex index : indices) {
			BlockEntry entry = entries.get(index);
			entry.offset = nextOffset;
			entry.serialize(buf);
			nextOffset += entry.contents.length;
			
			int pos = buf.position();
			buf.position((int) entry.offset);
			buf.put(entry.contents, entry.contentsOffset, entry.length);
			buf.position(pos);
		}
		
		return buf.array();
	}
	
	protected void deserialize(byte[] data) throws InvalidBlockException {
		ByteBuffer buf = ByteBuffer.wrap(data);
		long numEntries = buf.getLong();
		buf.position(fixedHeaderLength());
		assertIntegrity(0 < numEntries && numEntries*indexEntryLength() <= Integer.MAX_VALUE);
		
		BlockEntry lastEntry = null;
		for(long i = 0; i < numEntries; i++) {
			BlockEntry entry = new BlockEntry(buf);
			if(lastEntry != null) {
				int length = entry.offset - lastEntry.offset;
				lastEntry.length = length;
			}
			
			lastEntry = entry;
		}
		
		lastEntry.setLength(data.length - lastEntry.offset);
	}
	
	protected Key textKey() {
		return fs.getArchive().getConfig().deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-text-key",
				fs.getArchive().getConfig().getArchiveId());
	}
	
	protected Key saltKey() {
		return fs.getArchive().getConfig().deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-salt-key",
				fs.getArchive().getConfig().getArchiveId());
	}
	
	protected Key authKey() {
		return fs.getArchive().getConfig().deriveKey(ArchiveAccessor.KEY_ROOT_SEED,
				"easysafe-page-auth-key",
				fs.getArchive().getConfig().getArchiveId());
	}

	private void assertIntegrity(boolean check) throws InvalidBlockException {
		if(!check) {
			throw new InvalidBlockException();
		}
	}
	
	@Override
	public boolean equals(Object other) {
		if(!(other instanceof Block)) return false;
		if(other == this) return true;
		return entries.equals(((Block) other).entries);
	}
}
