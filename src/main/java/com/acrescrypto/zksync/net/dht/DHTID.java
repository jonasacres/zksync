package com.acrescrypto.zksync.net.dht;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PublicDHKey;

public class DHTID implements Comparable<DHTID>, Sendable {
	protected BigInteger id;
	protected int        length;
	protected int        hash;
	
	public static DHTID zero(int length) {
		return new DHTID(BigInteger.valueOf(0), length);
	}
	
	public static DHTID max(int length) {
		return new DHTID(BigInteger.valueOf(2).pow(8*length), length);
	}
	
	public static DHTID withKey(PublicDHKey key) {
		return DHTID.withBytes(key.getCrypto().hash(key.getBytes()));
	}
	
	public static DHTID withBytes(byte[] id) {
		byte[] effectiveId = id;
		
		if((id[0] & 0x80) != 0) {
			// ensure that all numbers are considered positive.
			effectiveId = new byte[id.length + 1];
			System.arraycopy(id, 0, effectiveId, 1, id.length);
		}
		
		return new DHTID(new BigInteger(effectiveId), id.length);
	}
	
	public DHTID(BigInteger id, int length) {
		this.id     = id;
		this.length = length;
	}
	
	public DHTID flip() {
		byte[] bytes = serialize();
		for(int i = 0; i < bytes.length; i++) {
			bytes[i] ^= 0xff;
		}
		
		bytes[0] &= 0x7f;
		return DHTID.withBytes(bytes);
	}
	
	public DHTID xor(DHTID other) {
		return new DHTID(id.xor(other.id), length);
	}
	
	public DHTID midpoint(DHTID other) {
		BigInteger avg = id.add(other.id).shiftRight(1);
		return new DHTID(avg, length);
	}
	
	public DHTID randomLessThan(DHTID min) {
		BigInteger delta = id.add(min.id.negate());
		
		/* ideal is a uniform random number in [0, delta), which this isn't;
		 * we'll get something in the right range, but there is a slight bias.
		 */
		
		int    bitLen   = delta.bitLength();
		int    numBytes = (int) Math.ceil(((double) bitLen)/8.0);
		byte[] rnd      = CryptoSupport.defaultCrypto().rng(numBytes + 1);
		int    bitRem   = bitLen % 8;
		byte   mask     = (bitRem != 0)
				          ? (byte) ((1 << bitRem) - 1)
				          : (byte) 0;
		
		rnd[0]          = 0;    // guarantee positive
		rnd[1]         &= mask; // remove high-order bits that are obviously not legit
		BigInteger r    = new BigInteger(rnd),
		  negativeDelta = delta.negate();
		
		while(r.compareTo(delta) >= 0) {
			r = r.add(negativeDelta);
		}
		
		BigInteger x    = r.add(min.id);
		DHTID      res  = new DHTID(x, length);
		
		return res;
	}
	
	@Override
	public int hashCode() {
		if(hash != 0) return hash;
		
		byte[] bytes  = id.toByteArray();
		byte[] hashed = CryptoSupport
				.defaultCrypto()
				.hash(bytes);
		this  .hash   = ByteBuffer.wrap(hashed).getInt();
		return hash;
	}

	@Override
	public int compareTo(DHTID other) {
		return id.compareTo(other.id);
	}
	
	@Override
	public byte[] serialize() {
		/** BigInteger serializes to the minimum bytes to represent the number in two's
		 * complement, meaning we get a sign bit. Therefore, we can have an integer that is
		 * as short as 1 byte, or as long as length+1 bytes.
		 * 
		 * Care is taken to make sure we always initialize the BigInteger with a sign bit of 0,
		 * so it shows as a positive number. Therefore, the leading bits of the serialized BigInt
		 * will also be 0.
		 */
		
		byte[] serialized = new byte[length];
		byte[] fromBigInt = id.toByteArray();
		
		if(fromBigInt.length > length) {
			assert(fromBigInt.length == length + 1); // hopefully this just has room for a sign bit, adding an extra byte
			System.arraycopy(fromBigInt, 1, serialized, 0,      serialized.length);
		} else {
			int offset = serialized.length - fromBigInt.length;
			System.arraycopy(fromBigInt, 0, serialized, offset, fromBigInt.length);
		}
		
		return serialized;
	}
	
	@Override
	public boolean equals(Object o) {
		if(o instanceof DHTPeer) {
			return this.equals(((DHTPeer) o).id);
		}
		
		if(o instanceof DHTID) {
			return id.equals(((DHTID) o).id);
		}
		
		return false;
	}
	
	@Override
	public String toString() {
		return this.toFullString();
	}
	
	public String toFullString() {
		return id.toString(16);
	}
	
	public String toShortString() {
		return toString().substring(0, 7);
	}

	public int getLength() {
		return length;
	}

	public BigInteger id() {
		return id;
	}
}
