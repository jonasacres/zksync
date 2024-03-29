package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.StorageTagRewriteException;
import com.acrescrypto.zksync.utility.Util;

public class StorageTag implements Comparable<StorageTag> {
	protected CryptoSupport crypto;
	private Block block;
	private byte[] tagBytes;
	private boolean isBlank;
	private final static char[] hexArray = "0123456789abcdef".toCharArray();
	
	protected static byte[] pad(int hashLen, byte[] tagBytes) {
		// this breaks if the hash length is > 255 bytes, but honestly, who needs a hash that long?
		byte needed = (byte) (hashLen - tagBytes.length);
		if(needed == 0) return tagBytes;
		
		ByteBuffer buf = ByteBuffer.allocate(hashLen);
		buf.put(tagBytes);
		while(buf.hasRemaining()) {
			buf.put(needed);
		}
		
		return buf.array();
	}
	
	protected static byte[] unpad(byte[] paddedTagBytes) {
		byte padLen = paddedTagBytes[paddedTagBytes.length-1];
		int len = paddedTagBytes.length - padLen;
		
		// validate the padding
		if(padLen > paddedTagBytes.length) throw new SecurityException();
		for(int i = paddedTagBytes.length - padLen; i < paddedTagBytes.length; i++) {
			if(paddedTagBytes[i] != padLen) {
				throw new SecurityException();
			}
		}
		
		if(len <= 0) return new byte[0];
		ByteBuffer buf = ByteBuffer.allocate(len);
		buf.put(paddedTagBytes, 0, Math.min(paddedTagBytes.length, len));
		return buf.array();
	}
	
	public static StorageTag fromPaddedBytes(CryptoSupport crypto, byte[] paddedTagBytes) {
		byte[] unpadded = unpad(paddedTagBytes);
		return new StorageTag(crypto, unpadded);
	}
	
	public StorageTag(Block block) {
		this.crypto = block.getArchive().getCrypto();
		this.block = block;
	}
	
	public StorageTag(CryptoSupport crypto, byte[] tagBytes) {
		this.crypto = crypto;
		this.setTagBytes(tagBytes);
	}
	
	public StorageTag(CryptoSupport crypto, String tagPath) {
		byte[] tagBytes = Util.hexToBytes(tagPath.replace("/", ""));
		this.crypto = crypto;
		this.setTagBytes(tagBytes);
	}
	
	public StorageTag(Key key, byte[] content) {
		this.crypto = key.getCrypto();
		this.setTagBytes(key.authenticate(content));
	}
	
	public boolean isBlank() {
		return isBlank;
	}
	
	public boolean isImmediate() {
		if(!isFinalized()) {
			return block.isImmediate();
		}
		
		return tagBytes.length < crypto.hashLength();
	}
	
	public boolean isStored() {
		return !isBlank() && !isImmediate();
	}
	
	public long shortTagPreserialized() {
		return Util.shortTag(getTagBytesPreserialized());
	}
	
	public long shortTag() throws IOException {
		return Util.shortTag(getTagBytes());
	}
	
	public String path() throws IOException {
		getTagBytes();
		// credit: https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java#9855338
		char[] hexChars = new char[2*tagBytes.length + 2];
		int ofs = 0;
		
		for (int j = 0; j < tagBytes.length; j++ ) {
			int v = tagBytes[j] & 0xFF;
			hexChars[ofs + 2*j] = hexArray[v >>> 4];
			hexChars[ofs + 2*j + 1] = hexArray[v & 0x0F];
			if(j < 2) {
				hexChars[ofs + 2*j + 2] = '/';
				ofs++;
			}
		}

		return new String(hexChars);
	}
	
	public CryptoSupport getCrypto() {
		return crypto;
	}
	
	public boolean isFinalized() {
		return tagBytes != null;
	}
	
	public byte[] getTagBytesPreserialized() {
		assert(isFinalized());
		return tagBytes;
	}
	
	public byte[] getTagBytes() throws IOException {
		if(tagBytes == null && block != null) {
			// causes tagBytes to get set
			block.write();
		}
		
		return tagBytes;
	}
	
	public byte[] getPaddedTagBytes() throws IOException {
		if(!isImmediate()) {
			return getTagBytes();
		}
		
		return pad(crypto.hashLength(), getTagBytes());
	}
	
	public void setTagBytes(byte[] tagBytes) {
		if(this.tagBytes != null) {
			throw new StorageTagRewriteException(this);
		}
		
		this.tagBytes = tagBytes.clone();
		
		if(tagBytes.length == crypto.hashLength()) {
			for(byte b : tagBytes) {
				if(b != 0) {
					isBlank = false;
					return;
				}
			}
			
			isBlank = true;
		}
	}
	
	@Override
	public boolean equals(Object other) {
		if(!(other instanceof StorageTag)) {
			return false;
		}
		
		StorageTag o = (StorageTag) other;
		if((tagBytes == null) != (o.tagBytes == null)) {
			// one of us has bytes and the other doesn't
			return false;
		}
		
		if(tagBytes == null) {
			return block == o.block;
		}
		
		return Arrays.equals(tagBytes, o.tagBytes);
	}
	
	@Override
	public int compareTo(StorageTag other) {
		if(tagBytes == null) {
			if(other.tagBytes == null) {
				// this is entirely arbitrary but at least it's an answer
				return Integer.compareUnsigned(
						System.identityHashCode(this.block),
						System.identityHashCode(other.block)
				);
			}
			
			return 1; // tags without bytes always sort after tags with bytes
		} else if(other.tagBytes == null) {
			return -1;
		}
		
		return Arrays.compareUnsigned(tagBytes, other.tagBytes);
	}
	
	@Override
	public String toString() {
		int maxLen = 6;
		if(isImmediate()) {
			byte[] value = isFinalized() ? tagBytes : block.immediateValue();
			int len = Math.min(maxLen, value.length);
			return "tag-i" + value.length + "-" + Util.bytesToHex(value, len);
		}
		
		if(!isFinalized()) {
			return "tag-s-?";
		}
		
		return "tag-s-" + Util.bytesToHex(tagBytes, maxLen);
	}
	
	@Override
	public StorageTag clone() {
		if(block != null) {
			return new StorageTag(block);
		}
		
		return new StorageTag(crypto, tagBytes);
	}
	
	@Override
	public int hashCode() {
		if(isFinalized()) {
			if(tagBytes.length >= 4) {
				return ByteBuffer.wrap(tagBytes).getInt();
			} else {
				try {
					return ByteBuffer.wrap(getPaddedTagBytes()).getInt();
				} catch(IOException exc) {
					// shouldn't be possible if hasBytes() == true
					exc.printStackTrace();
					throw new RuntimeException(exc);
				}
			}
		}
		
		return System.identityHashCode(block);
	}

	public Block loadBlock(ZKArchive archive, boolean verifySignature) throws IOException {
		if(block == null) {
			assert(isFinalized() && isStored());
			block = new Block(archive, this, verifySignature);
		}
		
		return block;
	}
}
