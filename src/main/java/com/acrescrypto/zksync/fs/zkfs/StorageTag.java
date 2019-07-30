package com.acrescrypto.zksync.fs.zkfs;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.utility.Util;

public class StorageTag implements Comparable<StorageTag> {
	protected CryptoSupport crypto;
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
		return tagBytes.length < crypto.hashLength();
	}
	
	public boolean isStored() {
		return !isBlank() && !isImmediate();
	}
	
	public long shortTag() {
		return Util.shortTag(getTagBytes());
	}
	
	public String path() {
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
	
	public byte[] getTagBytes() {
		return tagBytes;
	}
	
	public byte[] getPaddedTagBytes() {
		if(!isImmediate()) {
			return getTagBytes();
		}
		
		return pad(crypto.hashLength(), getTagBytes());
	}
	
	public void setTagBytes(byte[] tagBytes) {
		this.tagBytes = tagBytes;
		
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
	
	public boolean equals(byte[] other) {
		return Arrays.equals(tagBytes, other);
	}
	
	@Override
	public boolean equals(Object other) {
		if(!(other instanceof StorageTag)) {
			return false;
		}
		
		byte[] oTagBytes = ((StorageTag) other).getTagBytes();
		return Arrays.equals(tagBytes, oTagBytes);
	}
	
	@Override
	public int compareTo(StorageTag other) {
		return Arrays.compare(tagBytes, other.getTagBytes());
	}
	
	@Override
	public String toString() {
		int maxLen = 6;
		if(isImmediate()) {
			int len = Math.min(maxLen, tagBytes.length);
			return "tag-i" + tagBytes.length + "-" + Util.bytesToHex(tagBytes, len);
		}
		
		return "tag-s-" + Util.bytesToHex(tagBytes, maxLen);
	}
	
	@Override
	public StorageTag clone() {
		return new StorageTag(crypto, getTagBytes());
	}
}
