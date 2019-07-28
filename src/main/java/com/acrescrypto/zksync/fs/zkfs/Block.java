package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SignedSecureFile;
import com.acrescrypto.zksync.exceptions.BlockDoesNotContainPageException;
import com.acrescrypto.zksync.exceptions.ClosedException;
import com.acrescrypto.zksync.exceptions.InsufficientCapacityException;
import com.acrescrypto.zksync.exceptions.InvalidBlockException;

public class Block {
	protected ZKArchive archive;
	protected int remainingCapacity;
	protected byte[] blockContents;
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
			return o.identity == identity && o.pageNum == pageNum && o.type == type;
		}

		@Override
		public int compareTo(BlockEntryIndex o) {
			int c = Long.compareUnsigned(identity, o.identity);
			if(c != 0) {
				return c;
			}
			
			c = Byte.compareUnsigned(type, o.type);
			if(c != 0) {
				return c;
			}
			
			return Long.compareUnsigned(pageNum, o.pageNum);
		}
	}
	
	public class BlockEntry {
		long identity;
		long pageNum;
		byte type;
		int offset;
		int length;
		byte[] contents;
		
		public BlockEntry(long identity, long pageNum, byte type) {
			this.identity = identity;
			this.pageNum = pageNum;
			this.type = type;
		}
		
		public BlockEntry(ByteBuffer buf) throws InvalidBlockException {
			this.identity = buf.getLong();
			this.type = buf.get();
			this.pageNum = buf.getLong();
			this.contents = blockContents;

			long longOffset = buf.getLong();
			
			assertIntegrity(0 <= this.pageNum);
			assertIntegrity(0 < longOffset);
			assertIntegrity(longOffset < archive.getConfig().getPageSize());
			
			this.offset = (int) longOffset;
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
				if(contents[offset+i] != o.contents[o.offset+i]) {
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
	
	public Block(ZKArchive archive) {
		this.archive = archive;
		this.remainingCapacity = archive.getConfig().getPageSize() - fixedHeaderLength();
		this.deferrableTag = new DeferrableTag(this);
		this.isWritable = true;
	}
	
	public Block(DeferrableTag tag) throws IOException {
		this.archive = tag.archive;
		this.deferrableTag = tag;
		load();
	}
	
	public void addData(long identity, long pageNum, byte type, byte[] contents, int offset, int length) throws IOException {
		if(!isWritable()) {
			throw new ClosedException();
		}
		
		if(!canFitData(contents.length)) {
			throw new InsufficientCapacityException();
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
		} else {
			remainingCapacity += entry.length + indexEntryLength();
		}
		
		entry.contents = contents;
		entry.offset = offset;
		entry.length = length;
		remainingCapacity -= length + indexEntryLength();
	}
	
	public boolean hasData(long identity, long chunkNum, byte type) {
		BlockEntryIndex index = new BlockEntryIndex(identity, chunkNum, type);
		return entries.containsKey(index);
	}
	
	public boolean removeData(long identity, long chunkNum, byte type) {
		BlockEntryIndex index = new BlockEntryIndex(identity, chunkNum, type);
		BlockEntry existing = entries.remove(index);
		if(existing != null) {
			remainingCapacity += existing.length + indexEntryLength();
		}
		
		return existing != null;
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
		byte[] tagBytes = SignedSecureFile.withParams(archive.getStorage(),
			textKey(),
			saltKey(),
			authKey(),
			archive.getConfig().getPrivKey())
		.write(serialize(), archive.getConfig().getPageSize());
		isWritable = false;
		archive.addPageTag(tagBytes);
		deferrableTag.setBytes(tagBytes);
		return tagBytes;
	}
	
	public boolean isWritable() {
		return isWritable;
	}
	
	public DeferrableTag getDeferrableTag() {
		return deferrableTag;
	}
	
	protected void load() throws IOException {
		blockContents = SignedSecureFile.withTag(deferrableTag.getBytes(),
				archive.getStorage(),
				textKey(),
				saltKey(),
				authKey(),
				archive.getConfig().getPubKey()).read();
		deserialize(blockContents);
	}
	
	protected byte[] serialize() {
		int nextOffset = fixedHeaderLength() + indexEntryLength()*entries.size();
		int neededSize = nextOffset;
		for(BlockEntry entry : entries.values()) {
			neededSize += entry.length;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(neededSize);
		
		buf.putLong(entries.size());
		buf.putLong(0); // reserved
		LinkedList<BlockEntryIndex> indices = new LinkedList<>(entries.keySet());
		indices.sort(null);
		
		for(BlockEntryIndex index : indices) {
			BlockEntry entry = entries.get(index);
			int srcOffset = entry.offset;
			entry.offset = nextOffset;
			entry.serialize(buf);
			nextOffset += entry.contents.length;
			
			int pos = buf.position();
			buf.position((int) entry.offset);
			buf.put(entry.contents, srcOffset, entry.length);
			buf.position(pos);
			
			entry.contents = buf.array();
		}
		
		return blockContents = buf.array();
	}
	
	protected void deserialize(byte[] data) throws InvalidBlockException {
		ByteBuffer buf = ByteBuffer.wrap(data);
		long numEntries = buf.getLong();
		buf.position(fixedHeaderLength());
		assertIntegrity(0 <= numEntries && numEntries*indexEntryLength() <= Integer.MAX_VALUE);
		
		BlockEntry lastEntry = null;
		for(long i = 0; i < numEntries; i++) {
			BlockEntry entry = new BlockEntry(buf);
			if(lastEntry != null) {
				int length = entry.offset - lastEntry.offset;
				lastEntry.length = length;
			}
			
			lastEntry = entry;
			BlockEntryIndex index = new BlockEntryIndex(entry.identity, entry.pageNum, entry.type);
			entries.put(index, entry);
		}
		
		if(lastEntry != null) {
			lastEntry.setLength(data.length - lastEntry.offset);
		}
	}
	
	protected Key textKey() {
		return archive.getConfig().deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-text-key",
				archive.getConfig().getArchiveId());
	}
	
	protected Key saltKey() {
		return archive.getConfig().deriveKey(ArchiveAccessor.KEY_ROOT_ARCHIVE,
				"easysafe-page-salt-key",
				archive.getConfig().getArchiveId());
	}
	
	protected Key authKey() {
		return archive.getConfig().deriveKey(ArchiveAccessor.KEY_ROOT_SEED,
				"easysafe-page-auth-key",
				archive.getConfig().getArchiveId());
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

	public int getRemainingCapacity() {
		return remainingCapacity;
	}
}
