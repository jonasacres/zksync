package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.utility.Util;

public class DHTRoutingTable {
	protected DHTClient client;
	protected ArrayList<DHTBucket> buckets = new ArrayList<>();
	private ArrayList<DHTPeer> allPeers = new ArrayList<>();
	private Logger logger = LoggerFactory.getLogger(DHTRoutingTable.class);

	public DHTRoutingTable(DHTClient client) {
		this.client = client;
		read();
		new Thread(()->freshenThread()).start();
	}
	
	public void markFresh(DHTPeer peer) {
		for(DHTBucket bucket : buckets) {
			if(bucket.peers.contains(peer)) {
				bucket.markFresh();
				break;
			}
		}
	}
	
	public void reset() {
		buckets.clear();
		for(int i = 0; i <= client.idLength(); i++) {
			buckets.add(new DHTBucket(client, i));
		}
	}
	
	public void freshen() {
		for(DHTBucket bucket : buckets) {
			if(!bucket.needsFreshening()) continue;
			client.lookup(bucket.randomIdInRange(), (results)->{}); // can ignore results; just doing search freshens routing table
		}
	}
	
	public Collection<DHTPeer> allPeers() {
		return allPeers;
	}
	
	public void suggestPeer(DHTPeer peer) {
		int order = peer.id.xor(client.id).order();
		DHTBucket bucket = buckets.get(order+1); // add 1 since an exact match has order -1
		if(bucket.hasCapacity()) {
			bucket.add(peer);
			allPeers.add(peer);
		}
	}
	
	protected void removedPeer(DHTPeer peer) {
		allPeers.remove(peer);
	}
	
	protected void freshenThread() {
		while(true) {
			try {
				Util.sleep(1000*60);
				freshen();
			} catch(Exception exc) {
				logger.error("DHT routing table freshen thread encountered exception", exc);
			}
		}
	}
	
	protected String path() {
		return "dht-routing-table";
	}
	
	protected void write() throws IOException {
		MutableSecureFile file = MutableSecureFile.atPath(client.storage, path(), client.routingTableKey());
		file.write(serialize(), 0);
	}
	
	protected void read() {
		reset();
		
		MutableSecureFile file = MutableSecureFile.atPath(client.storage, path(), client.routingTableKey());
		try {
			deserialize(ByteBuffer.wrap(file.read()));
		} catch(IOException exc) {
		}
	}
	
	protected byte[] serialize() {
		LinkedList<byte[]> pieces = new LinkedList<>();
		int totalLength = 0;
		
		for(DHTPeer peer : allPeers) {
			byte[] piece = peer.serialize();
			pieces.add(piece);
			totalLength += piece.length;
		}
		
		
		ByteBuffer buf = ByteBuffer.allocate(4+totalLength);
		buf.putInt((short) pieces.size());
		for(byte[] piece : pieces) {
			buf.put(piece);
		}

		assert(buf.remaining() == 0);
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer serialized) throws EINVALException {
		try {
			int numPeers = serialized.getInt();
			for(int i = 0; i < numPeers; i++) {
				DHTPeer peer = new DHTPeer(client, serialized);
				suggestPeer(peer);
			}
		} catch(BufferUnderflowException exc) {
			throw new EINVALException(path());
		}
	}
}
