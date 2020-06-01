package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.utility.Util;

public class DHTClientSerializer {
	protected DHTClient client;
	
	public DHTClientSerializer(DHTClient client) {
		this.client = client;
	}
	
	public void read() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(
				client.getMaster().getStorage(),
				path(),
				client.clientInfoKey()
			);
		deserialize(ByteBuffer.wrap(file.read()));
	}
	
	public void write() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(
				client.getMaster().getStorage(),
				path(),
				client.clientInfoKey()
			);
		file.write(serialize(), 0);
	}
	
	public void purge() throws IOException {
		unlinkIfExists(path());
		unlinkIfExists(client.getRecordStore().path());
		unlinkIfExists(client.getRoutingTable().path());
	}
	
	public void unlinkIfExists(String path) throws IOException {
		try {
			client.getStorage().unlink(path);
		} catch(ENOENTException exc) {}
	}
	
	public byte[] serialize() {
		PrivateDHKey key = client.getPrivateKey();
		Key tagKey       = client.tagKey();
		
		ByteBuffer buf   = ByteBuffer.allocate(
				   key.getBytes().length
				 + key.publicKey().getBytes().length
				 + tagKey.getRaw().length);
		buf.put(key.getBytes());
		buf.put(key.publicKey().getBytes());
		buf.put(tagKey.getRaw());
		assert(buf.remaining() == 0);
		
		return buf.array();
	}
	
	public void deserialize(ByteBuffer serialized) throws EINVALException {
		try {
			CryptoSupport crypto = client.getMaster().getCrypto();
			byte[] privKeyRaw    = new byte[crypto.asymPrivateDHKeySize()];
			byte[] pubKeyRaw     = new byte[crypto.asymPublicDHKeySize()];
			byte[] tagKeyRaw     = new byte[crypto.symKeyLength()];
			
			serialized.get(privKeyRaw);
			serialized.get(pubKeyRaw);
			serialized.get(tagKeyRaw);
			
			client.setPrivateKey(crypto.makePrivateDHKeyPair(privKeyRaw, pubKeyRaw));
			client.setTagKey    (new Key(crypto, tagKeyRaw));
			client.setId        (new DHTID(client.getPublicKey()));
		} catch(BufferUnderflowException exc) {
			throw new EINVALException(path());
		}
	}
	
	public String path() {
		ByteBuffer truncated = ByteBuffer.allocate(8);
		truncated.get(
				client.getMaster().getCrypto().hash(client.getNetworkId()),
				0,
				truncated.remaining()
			);
		
		return "dht-client-info-"+Util.bytesToHex(truncated.array());
	}
}
