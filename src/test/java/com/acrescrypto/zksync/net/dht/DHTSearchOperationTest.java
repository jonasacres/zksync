package com.acrescrypto.zksync.net.dht;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class DHTSearchOperationTest {
	class DummyClient extends DHTClient {
		ArrayList<DHTPeer> simPeers;

		public DummyClient() {
			this.crypto = new CryptoSupport();
			this.simPeers = makeTestList(this, 2048);
			this.routingTable = new DummyRoutingTable(this);
		}
		
		public ArrayList<DHTPeer> peerSample(int size) {
			ArrayList<DHTPeer> sample = new ArrayList<>(size);
			Shuffler shuffler = new Shuffler(simPeers.size());
			for(int i = 0; i < size; i++) {
				sample.add(simPeers.get(shuffler.next()));
			}
			return sample;
		}
	}
	
	class DummyRoutingTable extends DHTRoutingTable {
		ArrayList<DHTPeer> allPeers;
		DummyClient client;
		
		public DummyRoutingTable(DummyClient client) {
			super.client = this.client = client;
			this.allPeers = client.peerSample(64);
		}

		@Override public Collection<DHTPeer> allPeers() { return allPeers; }
		@Override public void reset() { this.allPeers = new ArrayList<>(0); }
	}
	
	class DummyPeer extends DHTPeer {
		DummyClient client;
		ArrayList<DHTPeer> knownPeers;
		
		int requestsReceived = 0;
		
		public DummyPeer(DummyClient client, String address, int port, byte[] pubKey) {
			super(client, address, port, pubKey);
			this.client = client;
		}
		
		@Override
		public void findNode(DHTID id, DHTFindNodeCallback callback) {
			if(knownPeers == null) {
				knownPeers = client.peerSample(128);
			}
			
			requestsReceived++;
			if(sendResponses) {
				ArrayList<DHTPeer> closest = closestInList(searchId, knownPeers, DHTSearchOperation.MAX_RESULTS);
				ArrayList<DHTPeer> lowerHalf = new ArrayList<>(), upperHalf = new ArrayList<>();
				
				for(DHTPeer peer : closest) {
					if(lowerHalf.size() <= upperHalf.size()) {
						lowerHalf.add(peer);
					} else {
						upperHalf.add(peer);
					}
				}
				
				Thread t1 = new Thread(()->callback.response(lowerHalf, false));
				Thread t2 = new Thread(()->{
					try { t1.join(); } catch (InterruptedException e) { }
					callback.response(upperHalf, true);
				});
				
				t1.start();
				t2.start();
			}
		}
	}
	
	DummyClient client;
	CryptoSupport crypto;
	DHTID searchId;
	DHTSearchOperation op;
	Collection<DHTPeer> results;
	boolean sendResponses = true;
	
	public void waitForResult() {
		assertTrue(Util.waitUntil(100, ()->results != null));
	}
	
	public ArrayList<DHTPeer> makeTestList(DummyClient client, int size) {
		ArrayList<DHTPeer> list = new ArrayList<>(size);
		for(int i = 0; i < size; i++) {
			list.add(makeTestPeer(client, i));
		}
		
		return list;
	}
	
	public ArrayList<DHTPeer> closestInList(DHTID id, Collection<DHTPeer> peers, int numResults) {
		ArrayList<DHTPeer> closest = new ArrayList<>(numResults);
		DHTID worstDistance = null;
		DHTPeer worstPeer = null;
		
		for(DHTPeer peer : peers) {
			DHTID distance = peer.id.xor(id);
			if(closest.size() >= numResults && distance.compareTo(worstDistance) < 0) {
				closest.remove(worstPeer);
				
				worstDistance = distance;
				worstPeer = peer;
				
				for(DHTPeer existing : closest) {
					DHTID existingDistance = existing.id.xor(id);
					if(existingDistance.compareTo(worstDistance) > 0) {
						worstDistance = existingDistance;
						worstPeer = existing;
					}
				}
				
				closest.add(peer);
			} else if(closest.size() < numResults) {
				if(worstDistance == null || distance.compareTo(worstDistance) > 0) {
					worstDistance = distance;
					worstPeer = peer;
				}
				
				closest.add(peer);
			}
		}
		
		return closest;
	}
	
	public DHTPeer makeTestPeer(DummyClient client, int i) {
		byte[] seed = ByteBuffer.allocate(4).putInt(i).array();
		byte[] pubKey = crypto.prng(seed).getBytes(crypto.asymPublicDHKeySize());
		return new DummyPeer(client, "10.0.0."+i, 1000+i, pubKey);
	}
	
	@Before
	public void beforeEach() {
		crypto = new CryptoSupport();
		client = new DummyClient();
		searchId = new DHTID(crypto.rng(crypto.hashLength()));
		op = new DHTSearchOperation(client, searchId, (results)->{this.results = results;});
	}
	
	@Test
	public void testConstructorSetsFields() {
		assertEquals(client, op.client);
		assertEquals(searchId, op.searchId);
		assertNotNull(op.callback);
	}
	
	@Test
	public void testRunSendsRequestToClosestPeers() {
		sendResponses = false;
		ArrayList<DHTPeer> closest = closestInList(searchId, client.routingTable.allPeers(), DHTSearchOperation.MAX_RESULTS);
		
		op.run();
		
		for(DHTPeer peer : client.routingTable.allPeers()) {
			if(((DummyPeer) peer).requestsReceived > 0) {
				assertTrue(closest.contains(peer));
			} else {
				assertFalse(closest.contains(peer));
			}
		}
	}
	
	@Test
	public void testRunSendsRequestRecursively() {
		int numSeen = 0;
		DHTID worstInitialDistance = null;
		for(DHTPeer peer : closestInList(searchId, client.routingTable.allPeers(), DHTSearchOperation.MAX_RESULTS)) {
			if(worstInitialDistance == null || worstInitialDistance.compareTo(peer.id.xor(searchId)) < 0) {
				worstInitialDistance = peer.id.xor(searchId);
			}
		}
		
		op.run();
		waitForResult();
		
		for(DHTPeer peer : client.simPeers) {
			if(((DummyPeer) peer).requestsReceived > 0) {
				assertTrue(worstInitialDistance.compareTo(peer.id.xor(searchId)) >= 0);
				numSeen++;
			}
		}
		
		assertTrue(numSeen > DHTSearchOperation.MAX_RESULTS);
	}
	
	@Test
	public void testRunInvokesCallbackWithBestResults() {
		ArrayList<DHTPeer> expectedResults = closestInList(searchId, client.simPeers, DHTSearchOperation.MAX_RESULTS);
		op.run();
		waitForResult();
		
		assertEquals(expectedResults.size(), results.size());
		assertTrue(expectedResults.containsAll(results));
	}
	
	@Test
	public void testRunMakesImmediateEmptyCallbackIfNoPeers() {
		client.routingTable.reset();
		op.run();
		waitForResult();
		assertEquals(0, results.size());
	}
}
