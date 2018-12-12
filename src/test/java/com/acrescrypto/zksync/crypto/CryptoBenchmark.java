package com.acrescrypto.zksync.crypto;

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.acrescrypto.zksync.Benchmarks;

import au.com.forward.sipHash.SipHash_2_4;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class CryptoBenchmark {
	static CryptoSupport crypto;
	static byte[] key;
	static byte[] iv;
	
	@BeforeClass
	public static void beforeAll() {
		crypto = CryptoSupport.defaultCrypto();
		key = crypto.rng(crypto.symKeyLength());
		iv = crypto.rng(crypto.symIvLength());
		crypto.encrypt(key, iv, new byte[1024], new byte[1024], -1); // prime the pump
		Benchmarks.beginBenchmarkSuite("Crypto");
	}
	
	@Test
	public void testSymmetricEncryptThroughputNoADNoPad() {
		byte[] oneMiB = new byte[1024*1024];
		
		Benchmarks.run("MiB", (i)->{
			crypto.encrypt(key, iv, oneMiB, null, -1);
		});
	}
	
	@Test
	public void testSymmetricEncryptThroughputNoADPadded() {
		byte[] almostOneMiB = new byte[1024*1023];
		
		Benchmarks.run("MiB", (i)->{
			crypto.encrypt(key, iv, almostOneMiB, null, 1024*1024);
		});
	}
	
	@Test
	public void testSymmetricEncryptThroughputWithADNoPad() {
		byte[] oneMiB = new byte[1024*1024];
		byte[] ad = crypto.rng(1024);
		
		Benchmarks.run("MiB", (i)->{
			crypto.encrypt(key, iv, oneMiB, ad, -1);
		});
	}
	
	@Test
	public void testSymmetricEncryptThroughputWithADPadded() {
		byte[] almostOneMiB = new byte[1024*1023];
		byte[] ad = crypto.rng(1024);
		
		Benchmarks.run("MiB", (i)->{
			crypto.encrypt(key, iv, almostOneMiB, ad, 1024*1024);
		});
	}
	
	//
	@Test
	public void testSymmetricDecryptThroughputNoADNoPad() {
		byte[] oneMiB = crypto.encrypt(key, iv, new byte[1024*1024], null, -1);
		
		Benchmarks.run("MiB", (i)->{
			crypto.decrypt(key, iv, oneMiB, null, false);
		});
	}
	
	@Test
	public void testSymmetricDecryptThroughputNoADPadded() {
		byte[] oneMiB = crypto.encrypt(key, iv, new byte[1024*1023], null, 1024*1024);
		
		Benchmarks.run("MiB", (i)->{
			crypto.decrypt(key, iv, oneMiB, null, true);
		});
	}
	
	@Test
	public void testSymmetricDecryptThroughputWithADNoPad() {
		byte[] ad = crypto.rng(1024);
		byte[] oneMiB = crypto.encrypt(key, iv, new byte[1024*1024], ad, -1);
		
		Benchmarks.run("MiB", (i)->{
			crypto.decrypt(key, iv, oneMiB, ad, true);
		});
	}
	
	@Test
	public void testSymmetricDecryptThroughputWithADPadded() {
		byte[] ad = crypto.rng(1024);
		byte[] oneMiB = crypto.encrypt(key, iv, new byte[1024*1023], ad, 1024*1024);
		
		Benchmarks.run("MiB", (i)->{
			crypto.decrypt(key, iv, oneMiB, ad, true);
		});
	}
	
	@Test
	public void testSymmetricCBCEncryptThroughput() {
		byte[] cbcIv = crypto.rng(crypto.symBlockSize());
		byte[] oneMiB = new byte[1024*1024];
		Benchmarks.run("MiB", (i)->{
			crypto.encryptCBC(key, cbcIv, oneMiB);
		});
	}
	
	@Test
	public void testSymmetricCBCDecryptThroughput() {
		byte[] cbcIv = crypto.rng(crypto.symBlockSize());
		byte[] oneMiB = crypto.encryptCBC(key, cbcIv, new byte[1024*1024]);
		Benchmarks.run("MiB", (i)->{
			crypto.decryptCBC(key, cbcIv, oneMiB);
		});
	}
	
	@Test
	public void testHashThroughput() {
		byte[] oneMiB = new byte[1024*1024];
		
		Benchmarks.run("MiB", (i)->{
			crypto.hash(oneMiB);
		});
	}
	
	@Test
	public void testSipHashThroughput() {
		byte[] key = crypto.rng(16);
		byte[] oneMiB = new byte[1024*1024];
		
		Benchmarks.run("MiB", (i)->{
			SipHash_2_4 siphash = new SipHash_2_4();
			siphash.hash(key, oneMiB);
			siphash.finish();
		});
	}
	
	@Test
	public void testAuthenticateThroughput() {
		byte[] oneMiB = new byte[1024*1024];
		
		Benchmarks.run("MiB", (i)->{
			crypto.authenticate(new byte[crypto.hashLength()], oneMiB);
		});
	}
	
	@Test
	public void testAsymmetricSignThroughput() {
		PrivateSigningKey key = crypto.makePrivateSigningKey();
		byte[] oneMiB = new byte[1024*1024];
		
		Benchmarks.run("MiB", (i)->{
			key.sign(oneMiB);
		});
	}
	
	@Test
	public void testAsymmetricVerifyThroughput() {
		PrivateSigningKey key = crypto.makePrivateSigningKey();
		PublicSigningKey pubKey = key.publicKey();
		byte[] oneMiB = new byte[1024*1024];
		byte[] signature = key.sign(oneMiB);
		
		Benchmarks.run("MiB", (i)->{
			pubKey.verify(oneMiB, signature);
		});
	}
	
	@Test
	public void testAsymmetricSecretDerivationPerformance() {
		PrivateDHKey privKey = crypto.makePrivateDHKey();
		PublicDHKey pubKey = crypto.makePrivateDHKey().publicKey();
		
		Benchmarks.run("secrets", (i)->{
			privKey.sharedSecret(pubKey);
		});
	}
}
