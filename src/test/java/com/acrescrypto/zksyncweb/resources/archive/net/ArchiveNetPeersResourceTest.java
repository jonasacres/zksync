package com.acrescrypto.zksyncweb.resources.archive.net;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedList;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.net.BlacklistEntry;
import com.acrescrypto.zksync.net.PeerAdvertisement;
import com.acrescrypto.zksync.net.PeerConnection;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.net.TCPPeerSocket;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XPeerInfo;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveNetPeersResourceTest {
	class DummySwarm extends PeerSwarm {
		LinkedList<PeerAdvertisement> addedAds = new LinkedList<>();
		ArrayList<DummyPeerConnection> connections = new ArrayList<>();
		PublicDHKey key;
		
		@Override
		public PublicDHKey getPublicIdentityKey() {
			if(key == null) key = archive.getCrypto().makePrivateDHKey().publicKey();
			return key;
		}
		
		@Override
		public void addPeerAdvertisement(PeerAdvertisement ad) {
			addedAds.add(ad);
		}
		
		@Override
		public Collection<PeerConnection> getConnections() {
			return new ArrayList<PeerConnection>(connections);
		}
		
		@Override public void close() {}
	}
	
	class DummyPeerConnection extends PeerConnection {
		boolean closed;
		int index;
		DummyPeerSocket socket;
		TCPPeerAdvertisement ad;
		
		public DummyPeerConnection(int index) {
			this.index = index;
			super.socket = this.socket = new DummyPeerSocket(swarm);
			PublicDHKey pubKey = new PublicDHKey(archive.getCrypto(),
					archive.getCrypto().expand(
						Util.serializeInt(index),
						archive.getCrypto().asymPublicDHKeySize(),
						new byte[0],
						new byte[0]));
			socket.isLocalRoleClient = index % 2 == 0;
			ad = socket.ad = new TCPPeerAdvertisement(pubKey,
					""+index,
					100*index,
					archive.getCrypto().hash(pubKey.getBytes()),
					index);
			socket.peerType = index % 3;
			localPaused = index % 2 == 0;
			remotePaused = ((index / 2) & 1) == 0;
			wantsEverything = ((index / 4) & 1) == 0;
			timeStart = 100*index;
		}
		
		@Override public void close() { closed = true; }
		@Override public int getPeerType() { return socket.peerType; }
	}
	
	class DummyPeerSocket extends TCPPeerSocket {
		boolean isLocalRoleClient;
		TCPPeerAdvertisement ad;
		int peerType;
		PublicDHKey pubKey;
		
		public DummyPeerSocket(DummySwarm swarm) {
			super();
			this.swarm = swarm;
		}
		
		@Override
		public BandwidthMonitor getMonitorRx() {
			return new DummyMonitor();
		}
		
		@Override
		public BandwidthMonitor getMonitorTx() {
			return new DummyMonitor();
		}
		
		@Override
		public TCPPeerAdvertisement getAd() {
			return ad;
		}
		
		@Override public String getAddress() { return ad.getHost(); }
		
		@Override
		public int getPeerType() {
			return peerType;
		}
		
		@Override public PeerSwarm getSwarm() { return swarm; }
	}
	
	class DummyMonitor extends BandwidthMonitor {
		@Override public long getBytesPerSecond() { return 1000; }
	}
	
    private HttpServer server;
    private ZKArchive archive;
    private WebTarget target;
    private String passphrase;
    private DummySwarm swarm;
    private String basepath;
    
    @BeforeClass
    public static void beforeAll() {
    	TestUtils.startDebugMode();
    	WebTestUtils.squelchGrizzlyLogs();
    }

    @SuppressWarnings("deprecation")
	@Before
    public void beforeEach() throws Exception {
    	State.setTestState();
        server = Main.startServer();
        Client c = ClientBuilder.newClient();
        target = c.target(Main.BASE_URI);
        passphrase = "passphrase";
        Util.setCurrentTimeMillis(0);
        
    	archive = State.sharedState().getMaster().createDefaultArchive(passphrase.getBytes());
    	State.sharedState().addOpenConfig(archive.getConfig());
    	swarm = new DummySwarm();
    	archive.getConfig().setSwarm(swarm);
    	
    	for(int i = 0; i < 64; i++) {
    		swarm.connections.add(new DummyPeerConnection(i));
    	}
    	
    	basepath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/net/peers/";
    }
    
    public String pathForPeer(int i) {
    	String base64 = Base64.getEncoder().encodeToString(swarm.connections.get(i).ad.getPubKey().getBytes());
    	return basepath + Util.toWebSafeBase64(base64);
    }
    
    public void validatePeerInfo(JsonNode info, int i) throws IOException {
    	DummyPeerConnection conn = swarm.connections.get(i);
    	int expectedRole = conn.getSocket().isLocalRoleClient() ? XPeerInfo.ROLE_REQUESTOR : XPeerInfo.ROLE_RESPONDER;

    	assertEquals(conn.socket.getMonitorRx().getBytesPerSecond(), info.get("bytesPerSecondRx").doubleValue(), 1e-5);
    	assertEquals(conn.socket.getMonitorTx().getBytesPerSecond(), info.get("bytesPerSecondTx").doubleValue(), 1e-5);
    	assertEquals(expectedRole, info.get("role").intValue());
    	assertEquals(conn.getPeerType(), info.get("peerType").intValue());
    	assertEquals(conn.isLocalPaused(), info.get("localPaused").booleanValue());
    	assertEquals(conn.isRemotePaused(), info.get("remotePaused").booleanValue());
    	assertEquals(conn.wantsEverything(), info.get("wantsEverything").booleanValue());
    	assertEquals(conn.getTimeStart(), info.get("timeStart").longValue());
    	
    	JsonNode ad = info.get("ad");
    	assertEquals(conn.ad.getHost(), ad.get("host").textValue());
    	assertEquals(conn.ad.getPort(), ad.get("port").intValue());
    	assertEquals(conn.ad.getVersion(), ad.get("version").intValue());
    	assertArrayEquals(conn.ad.getPubKey().getBytes(), ad.get("pubKey").binaryValue());
    	assertArrayEquals(conn.ad.getEncryptedArchiveId(), ad.get("encryptedArchiveId").binaryValue());
    }

    @After
    public void afterEach() throws Exception {
		Util.setCurrentTimeMillis(-1);
    	archive.close();
        server.shutdownNow();
        State.clearState();
    }
    
    @AfterClass
    public static void afterAll() {
    	TestUtils.stopDebugMode();
		TestUtils.assertTidy();
    }
    
    @Test
    public void testGetPeersReturns404ForNonexistentArchive() {
    	WebTestUtils.requestGetWithError(target, 404, "/archives/doesntexist/net/peers");
    }
    
    @Test
    public void testGetEmptyPeerList() {
    	swarm.connections.clear();
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(0, resp.get("peers").size());
    }
    
    @Test
    public void testGetNonemptyPeerList() throws IOException {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(swarm.connections.size(), resp.get("peers").size());
    	for(int i = 0; i < resp.get("peers").size(); i++) {
    		validatePeerInfo(resp.get("peers").get(i), i);
    	}
    }
    
    @Test
    public void testGetPeerReturns404ForNonexistentArchive() {
    	WebTestUtils.requestGetWithError(target, 404, "/archives/doesntexist/net/peers/alsodoesntexist");
    }
    
    @Test
    public void testGetPeerReturns404ForNonexistentPeer() {
    	WebTestUtils.requestGetWithError(target, 404, basepath + "doesntexist");
    }
    
    @Test
    public void testGetPeerReturnsInfoForRequestedPeer() throws IOException {
    	for(int i = 0; i < swarm.connections.size(); i++) {
	    	JsonNode resp = WebTestUtils.requestGet(target, pathForPeer(i));
	    	validatePeerInfo(resp, i);
    	}
    }
    
    @Test
    public void testDeletePeerReturns404ForNonexistentArchive() {
    	WebTestUtils.requestDeleteWithError(target, 404, "/archives/doesntexist/net/peers/alsodoesntexist");
    }
    
    @Test
    public void testDeletePeerReturns404ForNonexistentPeer() {
    	WebTestUtils.requestDeleteWithError(target, 404, basepath + "doesntexist");
    }
    
    @Test
    public void testDeletePeerDisconnectsPeer() {
    	for(int i = 0; i < swarm.connections.size(); i++) {
    		assertFalse(swarm.connections.get(i).closed);
	    	WebTestUtils.requestDelete(target, pathForPeer(i));
	    	assertTrue(swarm.connections.get(i).closed);
    	}
    }
    
    @Test
    public void testDeletePeerDoesNotBlacklistPeerIfNotRequested() {
    	for(int i = 0; i < swarm.connections.size(); i++) {
    		WebTestUtils.requestDelete(target, pathForPeer(i));
    		DummyPeerConnection conn = swarm.connections.get(i);
	    	assertFalse(archive.getMaster().getBlacklist().contains(conn.getSocket().getAddress()));
    	}
    }
    
    @Test
    public void testDeletePeerBlacklistsPeerIfRequested() {
    	for(int i = 0; i < swarm.connections.size(); i++) {
    		WebTestUtils.requestDelete(target, pathForPeer(i) + "?blacklist=1234");
    		DummyPeerConnection conn = swarm.connections.get(i);
    		
    		boolean found = false;
    		for(BlacklistEntry entry : archive.getMaster().getBlacklist().allEntries()) {
    			if(!entry.getAddress().equals(conn.getSocket().getAddress())) continue;
    			assertEquals(System.currentTimeMillis() + 1234000, entry.getExpiration(), 1000);
    			found = true;
    		}
    		
	    	assertTrue(found);
    	}
    }
    
    @Test
    public void testDeletePeerBlacklistsPeerForeverIfRequestedDurationIsMinusOne() {
    	for(int i = 0; i < swarm.connections.size(); i++) {
    		WebTestUtils.requestDelete(target, pathForPeer(i) + "?blacklist=-1");
    		DummyPeerConnection conn = swarm.connections.get(i);
    		
    		boolean found = false;
    		for(BlacklistEntry entry : archive.getMaster().getBlacklist().allEntries()) {
    			if(!entry.getAddress().equals(conn.getSocket().getAddress())) continue;
    			assertEquals(Long.MAX_VALUE, entry.getExpiration());
    			found = true;
    		}
    		
	    	assertTrue(found);
    	}
    }
}
