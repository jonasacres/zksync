package com.acrescrypto.zksyncweb.resources.archive.net;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

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
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.net.PeerSwarm;
import com.acrescrypto.zksync.net.RequestPool;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveNetPoolResourceTest {
	class DummyPool extends RequestPool {
		ArrayList<RevisionTag> revTags = new ArrayList<>();
		public DummyPool() throws IOException {
			ZKFS fs = archive.openBlank();
			for(int i = 0; i < 10; i++) {
				RevisionTag revTag = fs.commit();
				revTags.add(revTag);
				
				this.requestedInodes.add(i, new InodeRef(revTag, 100l*i));
				this.requestedRevisions.add(i, revTag);
				this.requestedRevisionDetails.add(i, revTag);
				
				for(int j = 0; j < 10; j++) {
					this.requestedPageTags.add(i, 10l*i + j);
				}
			}
		}
		
		@Override public int numPagesRequested() { return 10; }
		@Override public int numInodesRequested() { return 20; }
		@Override public int numRevisionsRequested() { return 30; }
		@Override public int numRevisionDetailsRequested() { return 40; }
		@Override public boolean isRequestingEverything() { return true; }
		@Override public boolean isPaused() { return true; }
	}
	
	class DummySwarm extends PeerSwarm {
		public DummySwarm(ZKArchiveConfig config) throws IOException {
			this.config = config;
			this.pool = new DummyPool();
		}
	}
	
    private HttpServer server;
    private ZKArchive archive;
    private WebTarget target;
    private String passphrase;
    private String basepath;
    private DummySwarm swarm;
    private DummyPool pool;

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
        swarm = new DummySwarm(archive.getConfig());
        pool = (DummyPool) swarm.getRequestPool();
    	archive.getConfig().setSwarm(swarm);
    	State.sharedState().addOpenConfig(archive.getConfig());
    	
    	basepath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/net/pool/";
    }

    @After
    public void afterEach() throws Exception {
    	archive.close();
        server.shutdownNow();
        State.clearState();
    }
    
    @AfterClass
    public static void afterAll() {
    	TestUtils.stopDebugMode();
    }

    @Test
    public void testGetShowsNumberOfPagesInPool() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(pool.numPagesRequested(), resp.get("numPages").intValue());
    }
    
    @Test
    public void testGetShowsNumberOfInodesInPool() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(pool.numInodesRequested(), resp.get("numInodes").intValue());
    }
    
    @Test
    public void testGetShowsNumberOfRevisionsInPool() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(pool.numRevisionsRequested(), resp.get("numRevisions").intValue());
    }
    
    @Test
    public void testGetShowsNumberOfRevisionDetailsInPool() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(pool.numRevisionDetailsRequested(), resp.get("numRevisionDetails").intValue());
    }
    
    @Test
    public void testGetShowsIsRequestingEverything() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(pool.isRequestingEverything(), resp.get("isRequestingEverything").booleanValue());
    }

    @Test
    public void testGetShowsPaused() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath);
    	assertEquals(pool.isPaused(), resp.get("paused").booleanValue());
    }
    
    @Test
    public void testGetPagesShowsQueuedPages() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath + "pages");
    	JsonNode pages = resp.get("pages");
    	HashSet<Integer> seenPriorities = new HashSet<>();
    	
    	pages.fieldNames().forEachRemaining((name)->{
    		assertTrue(name.matches("^[0-9]+$"));
    		int priority = Integer.parseInt(name);
    		assertTrue(0 <= priority && priority < 10);
    		seenPriorities.add(priority);
    		
    		HashSet<Long> seenTags = new HashSet<>();
    		pages.get(name).forEach((element)->{
    			long value = element.longValue();
    			assertTrue(10*priority <= value && value < 10*(priority+1));
    			assertFalse(seenTags.contains(value));
    			seenTags.add(value);
    		});
    		
    		assertEquals(10, seenTags.size());
    	});
    	
    	assertEquals(10, seenPriorities.size());
    }
    
    @Test
    public void testGetInodesShowsQueuedInodes() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath + "inodes");
    	JsonNode inodes = resp.get("inodes");
    	HashSet<Integer> seenPriorities = new HashSet<>();
    	
    	inodes.fieldNames().forEachRemaining((name)->{
    		assertTrue(name.matches("^[0-9]+$"));
    		int priority = Integer.parseInt(name);
    		assertTrue(0 <= priority && priority < 10);
    		seenPriorities.add(priority);
    		
    		assertEquals(1, inodes.get(name).size());
    		JsonNode element = inodes.get(name).get(0);

    		long inodeId = element.get("inodeId").longValue();
			byte[] revTag = null;
			try {
				revTag = element.get("revTag").binaryValue();
			} catch (IOException e) {
				fail();
			}
			
			assertEquals(100*priority, inodeId);
			assertArrayEquals(pool.revTags.get(priority).getBytes(), revTag);
    		
    	});
    	
    	assertEquals(10, seenPriorities.size());
    }
    
    @Test
    public void testGetRevisionsShowsQueuedRevisions() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath + "revisions");
    	JsonNode revisions = resp.get("revisions");
    	HashSet<Integer> seenPriorities = new HashSet<>();
    	
    	revisions.fieldNames().forEachRemaining((name)->{
    		assertTrue(name.matches("^[0-9]+$"));
    		int priority = Integer.parseInt(name);
    		assertTrue(0 <= priority && priority < 10);
    		seenPriorities.add(priority);
    		
    		assertEquals(1, revisions.get(name).size());
    		JsonNode element = revisions.get(name).get(0);

			byte[] revTag = null;
			try {
				revTag = element.binaryValue();
			} catch (IOException e) {
				fail();
			}
			
			assertArrayEquals(pool.revTags.get(priority).getBytes(), revTag);
    		
    	});
    	
    	assertEquals(10, seenPriorities.size());
    }

    @Test
    public void testGetRevisionDetailsShowsQueuedRevisionDetails() {
    	JsonNode resp = WebTestUtils.requestGet(target, basepath + "revisiondetails");
    	JsonNode revisions = resp.get("revisionDetails");
    	HashSet<Integer> seenPriorities = new HashSet<>();
    	
    	revisions.fieldNames().forEachRemaining((name)->{
    		assertTrue(name.matches("^[0-9]+$"));
    		int priority = Integer.parseInt(name);
    		assertTrue(0 <= priority && priority < 10);
    		seenPriorities.add(priority);
    		
    		assertEquals(1, revisions.get(name).size());
    		JsonNode element = revisions.get(name).get(0);

			byte[] revTag = null;
			try {
				revTag = element.binaryValue();
			} catch (IOException e) {
				fail();
			}
			
			assertArrayEquals(pool.revTags.get(priority).getBytes(), revTag);
    		
    	});
    	
    	assertEquals(10, seenPriorities.size());
    }
}

