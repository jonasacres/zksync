package com.acrescrypto.zksync.net.noise;

import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

import au.com.forward.sipHash.SipHash_2_4;

public class SipObfuscator {
	// credit to ntcp2 project for this technique
	// https://geti2p.net/spec/ntcp2#siphash-obfuscated-length
	
	public final static String SIP_OBFUSCATOR_ASK_NAME = "siphash-length";
	protected CryptoSupport crypto;
	
	public class SipState {
		long iv;
		byte[] key;
		
		public SipState(byte[] hash) {
			key = new byte[16];
			ByteBuffer buf = ByteBuffer.wrap(crypto.expand(hash, 16 + 8, new byte[0], "siphash".getBytes()));
			buf.get(key);
			iv = buf.getLong();
		}
		
		public int obfuscate2(int data) {
			return (int) ((nextKey() ^ data) & 0xFFFF);
		}
		
		protected long nextKey() {
			SipHash_2_4 siphash = new SipHash_2_4();
			siphash.hash(key, Util.serializeLong(iv));
			return iv = siphash.finish();
		}
	}
	
	SipState read, write;
	
	public SipObfuscator(byte[] ikm, boolean isInitiator) {
		crypto = CryptoSupport.defaultCrypto();
		byte[] key1 = crypto.authenticate(ikm, new byte[] { 0x01 });
		byte[] key2 = crypto.authenticate(ikm, Util.concat(key1, new byte[] { 0x02 }));
		
		if(isInitiator) {
			read = new SipState(key1);
			write = new SipState(key2);
		} else {
			write = new SipState(key1);
			read = new SipState(key2);
		}
	}
	
	public SipState read() {
		return read;
	}
	
	public SipState write() {
		return write;
	}
}
