package com.acrescrypto.zksync.crypto;

import static org.junit.Assert.*;

import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Util;

public class CryptoSupportTest  {
	CryptoSupport crypto = CryptoSupport.defaultCrypto();

	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}

	@AfterClass
	public static void afterClass() {
		TestUtils.assertTidy();
	}

	@Test
	public void testHash() {
		byte[] digest = crypto.hash("a".getBytes());
		// BLAKE2b test vector, taken from https://github.com/openssl/openssl/blob/2d0b44126763f989a4cbffbffe9d0c7518158bb7/test/evptests.txt
		// HashContext has more test vectors; we're just making sure we're really using HashContext from CryptoSupport
		byte[] testVector = Util.hexToBytes("333fcb4ee1aa7c115355ec66ceac917c8bfd815bf7587d325aec1864edd24e34d5abe2c6b1b5ee3face62fed78dbef802f2a85cb91d455a8f5249d330853cb3c");
		assertTrue(Arrays.equals(digest, testVector));
	}

	@Test
	public void testAuthenticate() {
		byte[][][] vectors = blake2HmacTestVectors();

		for(int i = 0; i < vectors.length; i++) {
			byte[] digest = crypto.authenticate(vectors[i][1], vectors[i][0]);
			assertTrue(Arrays.equals(digest, vectors[i][2]));
		}
	}

	@Test
	public void testExpandWithSalt() {
		byte[][][] vectors = blake2HkdfTestVectors();

		for(byte[][] vector : vectors) {
			byte[] okm = crypto.expand(vector[0], vector[3].length, vector[1], vector[2]);
			assertTrue(Arrays.equals(okm, vector[3]));
		}
	}

	protected byte[] num2str(int n, int len) {
		byte[] str = new byte[len/8];
		int i = 0;
		while(n != 0) {
			str[str.length-i-1] = (byte) (n);
			i++;
			n >>>= 8;
		}

		return str;
	}

	protected byte[] concat(byte[] a, byte[] b) {
		byte[] c = new byte[a.length + b.length];
		for(int i = 0; i < a.length; i++) c[i] = a[i];
		for(int i = 0; i < b.length; i++) c[i+a.length] = b[i];
		return c;
	}

	protected byte[] aeadEncrypt(byte[] key, byte[] nonce, byte[] associatedData, byte[] plaintext) {
		return crypto.encrypt(key, nonce, plaintext, associatedData, -1);
	}

	@Test
	public void testEncryptPadding() {
		byte[] key = crypto.rng(crypto.symKeyLength()),
				iv = crypto.rng(crypto.symIvLength()),
				plaintext = "nice day out".getBytes();
		byte[] unpadded = crypto.encrypt(key, iv, plaintext, null, 0);
		byte[] padded = crypto.encrypt(key, iv, plaintext, null, 65536);
		assertTrue((unpadded.length <= 64));
		assertTrue((padded.length >= 65536));
	}

	@Test
	public void testDecryptPadding() {
		byte[] key = crypto.rng(crypto.symKeyLength()),
				iv = crypto.rng(crypto.symIvLength()),
				plaintext = "nice day out".getBytes(),
				ciphertext = crypto.encrypt(key, iv, plaintext, null, 65536),
				recovered = crypto.decrypt(key, iv, ciphertext, null, true);
		assertTrue(Arrays.equals(recovered, plaintext));
	}

	@Test
	public void testDecryptWithoutPadding() {
		byte[] key = crypto.rng(crypto.symKeyLength()),
				iv = crypto.rng(crypto.symIvLength()),
				plaintext = "nice day out".getBytes(),
				recovered =crypto.decrypt(key, iv, crypto.encrypt(key, iv, plaintext, null, 0), null, true);
		assertTrue(Arrays.equals(recovered, plaintext));
	}

	@Test
	public void testKeyDerivation() {
		byte[] derived = crypto.deriveKeyFromPassphrase("test".getBytes(), CryptoSupport.PASSPHRASE_SALT);

		/* This is a non-standard test vector. Generated using:
		 *   git clone https://github.com/P-H-C/phc-winner-argon2
		 *   cd phc-winner-argon2
		 *   git checkout 6c8653c3b6859a2d21850fa5979327feee8ca3ee # (latest commit to master branch at time of writing)
		 *   make
		 *   echo -n test | time ./argon2 easysafe-argon2-salt -d -t 32 -k 65536 -p 1 -l 32 -r # output is our expected result
		 */
		byte[] expected = Util.hexToBytes("d9db1306083ea308833feea8605a8c755a166157494add29af482dddfd074b07");
		assertTrue(Arrays.equals(derived, expected));
	}

	@Test
	public void testSymLengths() {
		byte[] pt = new byte[crypto.symBlockSize()], iv = crypto.makeSymmetricIv();
		byte[] key = crypto.makeSymmetricKey();

		assertEquals(key.length, crypto.symKeyLength());
		assertEquals(iv.length, crypto.symIvLength());

		byte[] ct = crypto.encrypt(key, iv, pt, new byte[0], -1);
		byte[] pt2 = crypto.decrypt(key, iv, ct, new byte[0], false);

		assertEquals(crypto.symBlockSize() + crypto.symTagLength(), ct.length);
		assertTrue(Arrays.equals(pt, pt2));
	}

	@Test
	public void testHashLength() {
		assertEquals(crypto.hashLength(), crypto.hash(new byte[0]).length);
	}

	@Test
	public void testAsymSigningKeySizes() {
		PrivateSigningKey key = crypto.makePrivateSigningKey();
		assertEquals(crypto.asymPrivateSigningKeySize(), key.getBytes().length);
		assertEquals(crypto.asymPublicSigningKeySize(), key.publicKey().getBytes().length);
		assertEquals(crypto.asymSignatureSize(), key.sign(new byte[0]).length);
	}

	@Test
	public void testAsymDHKeySizes() {
		PrivateDHKey key = crypto.makePrivateDHKey(), key2 = crypto.makePrivateDHKey();
		assertEquals(crypto.asymPrivateDHKeySize(), key.getBytes().length);
		assertEquals(crypto.asymPublicDHKeySize(), key.publicKey().getBytes().length);
		assertEquals(crypto.asymDHSecretSize(), key.sharedSecret(key2.publicKey()).length);
	}

	@Test
	public void testSymPaddedCiphertextSize() {
		byte[] key = crypto.makeSymmetricKey(), iv = crypto.makeSymmetricIv();
		byte[] plaintext = new byte[1];
		for(int len = plaintext.length; len < 2*crypto.symBlockSize(); len++) {
			int actualCtLen = crypto.encrypt(key, iv, plaintext, new byte[0], len).length;
			assertEquals(crypto.symPaddedCiphertextSize(len), actualCtLen);			
		}
	}

	@Test
	public void testAsymSignature() {
		byte[] msg = "genuine article".getBytes();
		PrivateSigningKey key = crypto.makePrivateSigningKey(), key2 = crypto.makePrivateSigningKey();

		byte[] signature = key.sign(msg);

		assertTrue(key.publicKey().verify(msg, signature));
		assertTrue(key.verify(msg, signature));

		assertFalse(key2.publicKey().verify(msg, signature));
		assertFalse(key2.verify(msg, signature));

		signature[1] ^= 0x01;

		assertFalse(key.publicKey().verify(msg, signature));
		assertFalse(key.verify(msg, signature));
	}

	@Test
	public void testAsymDH() {
		PrivateDHKey[] keys = new PrivateDHKey[3];
		for(int i = 0; i < keys.length; i++) {
			keys[i] = crypto.makePrivateDHKey();
		}

		byte[] secret01 = keys[0].sharedSecret(keys[1].pubKey);
		byte[] secret10 = keys[1].sharedSecret(keys[0].pubKey);
		byte[] secret02 = keys[0].sharedSecret(keys[2].pubKey);
		byte[] secret20 = keys[2].sharedSecret(keys[0].pubKey);
		byte[] secret12 = keys[1].sharedSecret(keys[2].pubKey);
		byte[] secret21 = keys[2].sharedSecret(keys[1].pubKey);

		assertTrue(Arrays.equals(secret01, secret10));
		assertTrue(Arrays.equals(secret02, secret20));
		assertTrue(Arrays.equals(secret21, secret12));

		assertFalse(Arrays.equals(secret01, secret02));
		assertFalse(Arrays.equals(secret01, secret12));
		assertFalse(Arrays.equals(secret02, secret12));
	}

	@Test
	public void testEd25519SigningTestVectors() {
		{ // RFC 8032 7.1, Test 1
			byte[] rawKey = Util.hexToBytes("9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60");
			byte[] expectedPubKey = Util.hexToBytes("d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a");
			byte[] expectedSignature = Util.hexToBytes("e5564300c360ac729086e2cc806e828a84877f1eb8e5d974d873e065224901555fb8821590a33bacc61e39701cf9b46bd25bf5f0595bbe24655141438e7a100b");
			byte[] testData = new byte[0];

			PrivateSigningKey key = crypto.makePrivateSigningKey(rawKey);

			assertTrue(Arrays.equals(rawKey, key.getBytes()));
			assertTrue(Arrays.equals(expectedPubKey, key.publicKey().getBytes()));
			assertTrue(Arrays.equals(expectedSignature, key.sign(testData)));
		}

		{ // RFC 8032 7.1, Test 2
			byte[] rawKey = Util.hexToBytes("4ccd089b28ff96da9db6c346ec114e0f5b8a319f35aba624da8cf6ed4fb8a6fb");
			byte[] expectedPubKey = Util.hexToBytes("3d4017c3e843895a92b70aa74d1b7ebc9c982ccf2ec4968cc0cd55f12af4660c");
			byte[] expectedSignature = Util.hexToBytes("92a009a9f0d4cab8720e820b5f642540a2b27b5416503f8fb3762223ebdb69da085ac1e43e15996e458f3613d0f11d8c387b2eaeb4302aeeb00d291612bb0c00");
			byte[] testData = new byte[] { 0x72 };

			PrivateSigningKey key = crypto.makePrivateSigningKey(rawKey);

			assertTrue(Arrays.equals(rawKey, key.getBytes()));
			assertTrue(Arrays.equals(expectedPubKey, key.publicKey().getBytes()));
			assertTrue(Arrays.equals(expectedSignature, key.sign(testData)));
		}

		{ // RFC 8032 7.1, Test 3
			byte[] rawKey = Util.hexToBytes("c5aa8df43f9f837bedb7442f31dcb7b166d38535076f094b85ce3a2e0b4458f7");
			byte[] expectedPubKey = Util.hexToBytes("fc51cd8e6218a1a38da47ed00230f0580816ed13ba3303ac5deb911548908025");
			byte[] expectedSignature = Util.hexToBytes("6291d657deec24024827e69c3abe01a30ce548a284743a445e3680d7db5ac3ac18ff9b538d16f290ae67f760984dc6594a7c15e9716ed28dc027beceea1ec40a");
			byte[] testData = new byte[] { (byte) 0xaf, (byte) 0x82 };

			PrivateSigningKey key = crypto.makePrivateSigningKey(rawKey);

			assertTrue(Arrays.equals(rawKey, key.getBytes()));
			assertTrue(Arrays.equals(expectedPubKey, key.publicKey().getBytes()));
			assertTrue(Arrays.equals(expectedSignature, key.sign(testData)));
		}

		{ // RFC 8032 7.1, Test 1024
			byte[] rawKey = Util.hexToBytes("f5e5767cf153319517630f226876b86c8160cc583bc013744c6bf255f5cc0ee5");
			byte[] expectedPubKey = Util.hexToBytes("278117fc144c72340f67d0f2316e8386ceffbf2b2428c9c51fef7c597f1d426e");
			byte[] expectedSignature = Util.hexToBytes("0aab4c900501b3e24d7cdf4663326a3a87df5e4843b2cbdb67cbf6e460fec350aa5371b1508f9f4528ecea23c436d94b5e8fcd4f681e30a6ac00a9704a188a03");
			byte[] testData = Util.hexToBytes("08b8b2b733424243760fe426a4b54908632110a66c2f6591eabd3345e3e4eb98fa6e264bf09efe12ee50f8f54e9f77b1e355f6c50544e23fb1433ddf73be84d879de7c0046dc4996d9e773f4bc9efe5738829adb26c81b37c93a1b270b20329d658675fc6ea534e0810a4432826bf58c941efb65d57a338bbd2e26640f89ffbc1a858efcb8550ee3a5e1998bd177e93a7363c344fe6b199ee5d02e82d522c4feba15452f80288a821a579116ec6dad2b3b310da903401aa62100ab5d1a36553e06203b33890cc9b832f79ef80560ccb9a39ce767967ed628c6ad573cb116dbefefd75499da96bd68a8a97b928a8bbc103b6621fcde2beca1231d206be6cd9ec7aff6f6c94fcd7204ed3455c68c83f4a41da4af2b74ef5c53f1d8ac70bdcb7ed185ce81bd84359d44254d95629e9855a94a7c1958d1f8ada5d0532ed8a5aa3fb2d17ba70eb6248e594e1a2297acbbb39d502f1a8c6eb6f1ce22b3de1a1f40cc24554119a831a9aad6079cad88425de6bde1a9187ebb6092cf67bf2b13fd65f27088d78b7e883c8759d2c4f5c65adb7553878ad575f9fad878e80a0c9ba63bcbcc2732e69485bbc9c90bfbd62481d9089beccf80cfe2df16a2cf65bd92dd597b0707e0917af48bbb75fed413d238f5555a7a569d80c3414a8d0859dc65a46128bab27af87a71314f318c782b23ebfe808b82b0ce26401d2e22f04d83d1255dc51addd3b75a2b1ae0784504df543af8969be3ea7082ff7fc9888c144da2af58429ec96031dbcad3dad9af0dcbaaaf268cb8fcffead94f3c7ca495e056a9b47acdb751fb73e666c6c655ade8297297d07ad1ba5e43f1bca32301651339e22904cc8c42f58c30c04aafdb038dda0847dd988dcda6f3bfd15c4b4c4525004aa06eeff8ca61783aacec57fb3d1f92b0fe2fd1a85f6724517b65e614ad6808d6f6ee34dff7310fdc82aebfd904b01e1dc54b2927094b2db68d6f903b68401adebf5a7e08d78ff4ef5d63653a65040cf9bfd4aca7984a74d37145986780fc0b16ac451649de6188a7dbdf191f64b5fc5e2ab47b57f7f7276cd419c17a3ca8e1b939ae49e488acba6b965610b5480109c8b17b80e1b7b750dfc7598d5d5011fd2dcc5600a32ef5b52a1ecc820e308aa342721aac0943bf6686b64b2579376504ccc493d97e6aed3fb0f9cd71a43dd497f01f17c0e2cb3797aa2a2f256656168e6c496afc5fb93246f6b1116398a346f1a641f3b041e989f7914f90cc2c7fff357876e506b50d334ba77c225bc307ba537152f3f1610e4eafe595f6d9d90d11faa933a15ef1369546868a7f3a45a96768d40fd9d03412c091c6315cf4fde7cb68606937380db2eaaa707b4c4185c32eddcdd306705e4dc1ffc872eeee475a64dfac86aba41c0618983f8741c5ef68d3a101e8a3b8cac60c905c15fc910840b94c00a0b9d0");

			PrivateSigningKey key = crypto.makePrivateSigningKey(rawKey);

			assertTrue(Arrays.equals(rawKey, key.getBytes()));
			assertTrue(Arrays.equals(expectedPubKey, key.publicKey().getBytes()));
			assertTrue(Arrays.equals(expectedSignature, key.sign(testData)));
		}
	}

	@Test
	public void testX25519DHTestVectors() {
		// RFC 7748 https://tools.ietf.org/html/rfc7748.html#page-14
		byte[] alicePubKey = Util.hexToBytes("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a");
		byte[] alicePrivKey = Util.hexToBytes("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a");
		byte[] bobPubKey = Util.hexToBytes("de9edb7d7b7dc1b4d35b61c2ece435373f8343c85b78674dadfc7e146f882b4f");
		byte[] bobPrivKey = Util.hexToBytes("5dab087e624a8a4b79e17f8b83800ee66f3bb1292618b6fd1c2f8b27ff88e0eb");
		byte[] expectedSecret = Util.hexToBytes("4a5d9d5ba4ce2de1728e3bf480350f25e07e21c947d19e3376f09b3c1e161742");

		PrivateDHKey alice = crypto.makePrivateDHKeyPair(alicePrivKey, alicePubKey);
		PrivateDHKey bob = crypto.makePrivateDHKeyPair(bobPrivKey, bobPubKey);

		assertTrue(Arrays.equals(expectedSecret, alice.sharedSecretRaw(bob.publicKey())));
		assertTrue(Arrays.equals(expectedSecret, bob.sharedSecretRaw(alice.publicKey())));
	}

	private byte[][][] blake2HkdfTestVectors() {
		// Test vectors for CryptoSupport.expand (HKDF), used in CryptoSupportTest.blake2HkdfTestVectors()
		return new byte[][][] {
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a74736")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a747360e940b2d430e3b51c384c11676d69fe6")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a747360e940b2d430e3b51c384c11676d69fe69a9366b5a746e6c8996cbb70c1da57e4d6f73835d8730d9c458feb1c843d9433")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a747360e940b2d430e3b51c384c11676d69fe69a9366b5a746e6c8996cbb70c1da57e4d6f73835d8730d9c458feb1c843d9433ebd9b589fba37e730c5deaac3634bb62797bf3f2ea17439d7da2915e19fb8da920449a5dffe6d36c2d80538a8fdb5c663e783659b41d62e1f01b604be1de2e9e")
			},
			{
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("7b603c9b16db2d6dbb14ed55fb3819a1")
			},
			{
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("7b603c9b16db2d6dbb14ed55fb3819a133209e5fd6fac4866392cf80f50583a9")
			},
			{
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("7b603c9b16db2d6dbb14ed55fb3819a133209e5fd6fac4866392cf80f50583a9b231120383d4d42414a4485c474c5d5735e846a87078a2353a4533f51f0749c3")
			},
			{
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("7b603c9b16db2d6dbb14ed55fb3819a133209e5fd6fac4866392cf80f50583a9b231120383d4d42414a4485c474c5d5735e846a87078a2353a4533f51f0749c3d828218ca1029e8e856758ad83b203a9be98391061f3eabfefb863f99cb11cac0293c10ed4389f92f747de524886e83eef0221785f16504d8a11457e3f771a80")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a74736")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a747360e940b2d430e3b51c384c11676d69fe6")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a747360e940b2d430e3b51c384c11676d69fe69a9366b5a746e6c8996cbb70c1da57e4d6f73835d8730d9c458feb1c843d9433")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes("5206a97d30367ecb2db33721e4a747360e940b2d430e3b51c384c11676d69fe69a9366b5a746e6c8996cbb70c1da57e4d6f73835d8730d9c458feb1c843d9433ebd9b589fba37e730c5deaac3634bb62797bf3f2ea17439d7da2915e19fb8da920449a5dffe6d36c2d80538a8fdb5c663e783659b41d62e1f01b604be1de2e9e")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes("9735016defdddae5a168b6a5999ef517")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes("9735016defdddae5a168b6a5999ef517aacc8c7190f5e5ecda59ecd62f75217b")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes("9735016defdddae5a168b6a5999ef517aacc8c7190f5e5ecda59ecd62f75217b069e8d4c64129a6424cad767325b2c22ea8aed5ec44b701be9bd379f3aacc917")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes("9735016defdddae5a168b6a5999ef517aacc8c7190f5e5ecda59ecd62f75217b069e8d4c64129a6424cad767325b2c22ea8aed5ec44b701be9bd379f3aacc91788990702b3e788729950ea7519a865721ccb434a63a433f365bd70a3ae1522778e3c6c18f55211f24c246bf3d280aaf0a12a82a1acb219903c661e2c3c9c9be3")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("057855ca3deef8ccb22610b5d9a5ac63")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("057855ca3deef8ccb22610b5d9a5ac631094482f5ad67cf6e12449ca42bf3df5")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("057855ca3deef8ccb22610b5d9a5ac631094482f5ad67cf6e12449ca42bf3df512488259cc92641e6190c03890a1bd45a7d9807ef51845424bcf122da4036a66")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("057855ca3deef8ccb22610b5d9a5ac631094482f5ad67cf6e12449ca42bf3df512488259cc92641e6190c03890a1bd45a7d9807ef51845424bcf122da4036a6601bc14ab3e8cca7507eb4f711f79697ac80af0bac1e9aaeb9a0f080a47866954077982c40bb957740b759975b87981488c51cece9b1699fa7d49d93ab6c2108b")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("8848892647a7a81370809fbdda2f094d")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("8848892647a7a81370809fbdda2f094da235371ece4d9fe961cc21094d6a682f")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("8848892647a7a81370809fbdda2f094da235371ece4d9fe961cc21094d6a682f1c2d70c9b65c998932c9dbe21e1da8c6b4e328bb9803ad9ba3af88d63643e5cd")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("8848892647a7a81370809fbdda2f094da235371ece4d9fe961cc21094d6a682f1c2d70c9b65c998932c9dbe21e1da8c6b4e328bb9803ad9ba3af88d63643e5cdd15428316f642ebc6c1468f25ef2c60820c69a2e415e100f11afcbdfef4000e9ce1eb32a88ce71259c707a0f0137800605f8f6ed7d3d2ebaac6b3eb16aa6aa07")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("41fb7b2b8b6ce343342f76eeb96e4b55")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("41fb7b2b8b6ce343342f76eeb96e4b55e88065bf52174730793e37584a111cd7")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("41fb7b2b8b6ce343342f76eeb96e4b55e88065bf52174730793e37584a111cd7690551dfdb9ba64acd287b278775510e0f35f6bf64942b97b39b79b3af5e1861")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("41fb7b2b8b6ce343342f76eeb96e4b55e88065bf52174730793e37584a111cd7690551dfdb9ba64acd287b278775510e0f35f6bf64942b97b39b79b3af5e186132c570e3837e5f274f610321906d6f775c20d3599b008e822a860f909206528e81fb3720cb1857ebd9fd50293e2e7ff6016b93cf6c3b3cea0c0c2b12c904a4eb")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("ff5c2221c09382f5aafcd5c9c5003d53")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("ff5c2221c09382f5aafcd5c9c5003d537a9a43be7430105863318b714cdfa822")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("ff5c2221c09382f5aafcd5c9c5003d537a9a43be7430105863318b714cdfa822997930fbd7394ecd8f9557d582ffe9e990321d4230f4e8c754fd4059fa173f53")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("ff5c2221c09382f5aafcd5c9c5003d537a9a43be7430105863318b714cdfa822997930fbd7394ecd8f9557d582ffe9e990321d4230f4e8c754fd4059fa173f53d10e1d9fabcd934d6c385bf6a61353d368c26cd4aa0ec2a33cbbdc6e91e9c010b2266dd31f3cc682adbec62577ad954e73869be8b82651899459e7df07ea40f9")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("127cb297c60c7380a372105590db2593")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("127cb297c60c7380a372105590db25931c26bf6ed7cad837ec9825841256fe4c")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("127cb297c60c7380a372105590db25931c26bf6ed7cad837ec9825841256fe4c3a8a829b815be8cfd305dc5b6cbaf8f8ad6abf489c1d00255d15345ac0cee2e6")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("127cb297c60c7380a372105590db25931c26bf6ed7cad837ec9825841256fe4c3a8a829b815be8cfd305dc5b6cbaf8f8ad6abf489c1d00255d15345ac0cee2e6226cf667ff6794d4a1f48f037b2128569b10cbe4e714f92f28f5957efb6c23d24fe0ec58996f874f68f8a39570268d8977b07066d16e239a9cf66546e21fc607")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("eccec7cc2288798c76a989b54412fa82")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("eccec7cc2288798c76a989b54412fa82b6090973ae04c4523be74f18b5039671")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("eccec7cc2288798c76a989b54412fa82b6090973ae04c4523be74f18b50396719d69f681d95589935ba2f20efd0c0591e64d9e5372c5b605ec3b21c1723a5f11")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes(""),
				Util.hexToBytes("eccec7cc2288798c76a989b54412fa82b6090973ae04c4523be74f18b50396719d69f681d95589935ba2f20efd0c0591e64d9e5372c5b605ec3b21c1723a5f1194afe3c289a4eb4a2a004aef9900396b73466961ccb7d188680adad4fbfc5f972f2a30e750e595e564a79bbb54ee0d1d82e4acdc0637633086c4cb424c2b7b63")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("910430626e0c7e9ce9c786533e7254bc")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("910430626e0c7e9ce9c786533e7254bcd9b36af369f3ffa2c73e1f856134a0d7")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("910430626e0c7e9ce9c786533e7254bcd9b36af369f3ffa2c73e1f856134a0d72b24ac161c7f8dcd38fb07e1433a3f218daab7618d57505f0a1f5ddb5d0ed74a")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes(""),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("910430626e0c7e9ce9c786533e7254bcd9b36af369f3ffa2c73e1f856134a0d72b24ac161c7f8dcd38fb07e1433a3f218daab7618d57505f0a1f5ddb5d0ed74a124765d25ddfa47be67e8bddbfa4e7cf89b10c1b7ed71c9b28c916530b4cbd239bdae1be212412b686d85876c1d510e2d16ac2d8f0e5aa57965bba89ea0a93e4")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("34547d9a4bbbe008f2d0803efcb2effb")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("34547d9a4bbbe008f2d0803efcb2effb3f3cd3f4f55aca244d8120b362a77dac")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("34547d9a4bbbe008f2d0803efcb2effb3f3cd3f4f55aca244d8120b362a77dac089e668fcad9fc6ad5febbcd63a13951112bf8f8217f0032e070b1fca716b18b")
			},
			{
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f"),
				Util.hexToBytes("101112131415161718191a1b1c1d1e1f"),
				Util.hexToBytes("34547d9a4bbbe008f2d0803efcb2effb3f3cd3f4f55aca244d8120b362a77dac089e668fcad9fc6ad5febbcd63a13951112bf8f8217f0032e070b1fca716b18b2cce3507aaa5b6c009acc6dc3b7ed11d5f16763b636df4c1637fe2e1b98858d8d3fb7cd077f2fd191218b2d0d60e543d83caa63cded13b7abe32d126fba8e89b")
			},
		};
	}

	private byte[][][] blake2HmacTestVectors() {
		// Generated from test-vectors.py, Python 3.6.5, commit db67d8c388d18cb428e257e42baf7c40682f9b83
		return new byte[][][] {
			{
				Util.hexToBytes(""),
				Util.hexToBytes(""),
				Util.hexToBytes("198cd2006f66ff83fbbd913f78aca2251caf4f19fe9475aade8cf2091b99a68466775177424f58286886cbae8229644cec747237d4b721735485e17372fdf59c")
			},
			{
				Util.hexToBytes(""),
				Util.hexToBytes("00"),
				Util.hexToBytes("198cd2006f66ff83fbbd913f78aca2251caf4f19fe9475aade8cf2091b99a68466775177424f58286886cbae8229644cec747237d4b721735485e17372fdf59c")
			},
			{
				Util.hexToBytes("00"),
				Util.hexToBytes(""),
				Util.hexToBytes("2ef45a97d499fab55b1617c0730ab7a204194893e242da28f788c598527c8d259a8037a7ab969846ba6765a77e7a202b44382efea835e3207de9cc552ab53e42")
			},
			{
				Util.hexToBytes("1011121314151617"),
				Util.hexToBytes("0001020304050607"),
				Util.hexToBytes("e11da32b8824ba977536d20aaab5b01d1a62575a2e1558ff6ed4c47ca957d0970ce560c523074a13ce04890ac9af838660521006d250d6e45de1dfcb2337f140")
			},
			{
				Util.hexToBytes("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9fa0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebfc0c1c2c3c4c5c6c7c8c9cacbcccdcecfd0d1d2d3d4d5d6d7d8d9dadbdcdddedfe0e1e2e3e4e5e6e7e8e9eaebecedeeeff0f1f2f3f4f5f6f7f8f9fafbfcfdfeff"),
				Util.hexToBytes("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f202122232425262728292a2b2c2d2e2f303132333435363738393a3b3c3d3e3f404142434445464748494a4b4c4d4e4f505152535455565758595a5b5c5d5e5f606162636465666768696a6b6c6d6e6f707172737475767778797a7b7c7d7e7f"),
				Util.hexToBytes("ba70bcc76775c4f7d51f823f6982a4ba15f166f921deff55b5f335da210a5344cf95895e3faa771ad4d8c20dca980073bd38554b499e85217fe7f27dde45350f")
			},
		};

	}
}
