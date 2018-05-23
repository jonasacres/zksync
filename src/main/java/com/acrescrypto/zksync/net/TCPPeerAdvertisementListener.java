package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.utility.Util;

public class TCPPeerAdvertisementListener {
	protected PeerSwarm swarm;
	protected CryptoSupport crypto;
	protected Logger logger = LoggerFactory.getLogger(TCPPeerSocketListener.class);
	protected int port;
	protected PrivateDHKey dhPrivateKey;
	
	public TCPPeerAdvertisementListener(PeerSwarm swarm, int port) {
		this.swarm = swarm;
		this.crypto = swarm.config.getAccessor().getMaster().getCrypto();
		this.port = port;
		initKeys();
		announce();
	}
	
	public boolean matchesKeyHash(PublicDHKey remotePubKey, byte[] keyHash) {
		ByteBuffer keyHashInput = ByteBuffer.allocate(2*crypto.asymPublicSigningKeySize());
		keyHashInput.put(remotePubKey.getBytes());
		keyHashInput.put(dhPrivateKey.publicKey().getBytes());
		Key keyHashKey = swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_SEED, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_SEED);
		
		byte[] expectedKeyHash = keyHashKey.authenticate(keyHashInput.array());
		return Arrays.equals(expectedKeyHash, keyHash);
	}
	
	public TCPPeerAdvertisement localAd() throws UnconnectableAdvertisementException {
		return new TCPPeerAdvertisement(dhPrivateKey.publicKey(), "localhost", port); // real hostname filled in by peers; use localhost as safe stand-in
	}
	
	public void announce() {
		new Thread(() -> {
			try {
				TCPPeerAdvertisement ad = localAd();
				swarm.advertiseSelf(ad);
			} catch(Exception exc) {
				logger.error("Announce thread caught exception", exc);
			}
		}).start();
	}
	
	protected MutableSecureFile storedFile() throws IOException {
		FS fs = swarm.config.getArchive().getMaster().localStorageFsForArchiveId(swarm.config.getArchiveId());
		return MutableSecureFile.atPath(fs, "tcp-identity", swarm.config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_AD_IDENTITY));
	}
	
	protected void initKeys() {
		try {
			deserialize(storedFile().read());
		} catch(ENOENTException exc) {
			try {
				this.dhPrivateKey = crypto.makePrivateDHKey();
				storedFile().write(serialize(), 0);
			} catch (IOException e) {
				logger.error("Caught exception writing advertisement for archive {}", Util.bytesToHex(swarm.config.getArchiveId()), exc);
			}
		} catch (IOException exc) {
			logger.error("Caught exception opening stored advertisement for archive {}", Util.bytesToHex(swarm.config.getArchiveId()), exc);
		}
	}
	
	protected byte[] serialize() {
		ByteBuffer buf = ByteBuffer.allocate(crypto.asymPrivateDHKeySize() + crypto.asymPublicDHKeySize());
		buf.put(dhPrivateKey.getBytes());
		buf.put(dhPrivateKey.publicKey().getBytes());
		assert(!buf.hasRemaining());
		return buf.array();
	}
	
	protected void deserialize(byte[] serialized) {
		byte[] privateKeyRaw = new byte[crypto.asymPrivateDHKeySize()];
		byte[] publicKeyRaw = new byte[crypto.asymPublicDHKeySize()];
		assert(serialized.length == privateKeyRaw.length + publicKeyRaw.length);
		ByteBuffer buf = ByteBuffer.wrap(serialized);
		buf.get(privateKeyRaw);
		buf.get(publicKeyRaw);
		assert(!buf.hasRemaining());
		
		dhPrivateKey = new PrivateDHKey(privateKeyRaw, publicKeyRaw);
	}
}