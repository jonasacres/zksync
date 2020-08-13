package com.acrescrypto.zksyncweb.resources.archive.revision;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashSet;
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
import com.acrescrypto.zksync.fs.zkfs.RevisionInfo;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveRevisionResourceTest {
	private HttpServer server;
	private WebTarget target;
	private ZKArchive archive;
	private ZKFS fs;
	private RevisionTag revTag;
	private String basePath;

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		WebTestUtils.squelchGrizzlyLogs();
	}

	@Before
	public void beforeEach() throws Exception {
		State.setTestState();
		server = Main.startServer(TestUtils.testHttpPort());
		Client c = ClientBuilder.newClient();
		target = c.target(TestUtils.testHttpUrl());

		archive = State.sharedState().getMaster().createDefaultArchive("passphrase".getBytes());
		archive.getConfig().advertise();
		archive.getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_READWRITE);
		State.sharedState().addOpenConfig(archive.getConfig());

		fs = State.sharedState().activeFs(archive.getConfig());
		revTag = fs.commit();
		fs.commit();
		basePath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/revisions/" + WebTestUtils.transformRevTag(revTag);
	}

	@After
	public void afterEach() throws Exception {
		fs.close();
		archive.close();
		server.shutdownNow();
		State.clearState();
	}

	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	public LinkedList<RevisionTag> makeRevisionTriangle(int maxHeight) throws IOException {
	    // don't let the default test setup screw things up for us
	    archive.getConfig().getRevisionList().clear();
	    archive.getConfig().getRevisionTree().clear();
	    
	    LinkedList<RevisionTag> results = new LinkedList<>();
	    
	    for(int i = 0; i < maxHeight; i++) {
	        try(ZKFS fs = archive.openBlank()) {
	            RevisionTag tag = null;
	            
    	        fs.write("conflict", Util.serializeInt(i));
    	        fs.write("" + i,     "value".getBytes());
    	        
    	        for(int j = 0; j <= i; j++) {
    	            fs.getInodeTable().setNextTitle("" + j);
    	            tag = fs.commit(); 
    	        }
    	        
    	        results.add(tag);
	        }
	    }
	    
	    return results;
	}
	
	public RevisionTag validateCanonizeJson(JsonNode resp, RevisionTag base, LinkedList<RevisionTag> parents) throws IOException {
        HashSet<RevisionTag> remainingParents = new HashSet<>(parents);
	    long                 expectedHeight   = 0;
        
        for(RevisionTag parent : parents) {
            expectedHeight = Math.max(expectedHeight, parent.getHeight()+1);
        }
        
        assertEquals(expectedHeight, resp.get("generation").longValue());
        assertEquals(parents.size(), resp.get("parents").size());
        
        for(JsonNode parentNode : resp.get("parents")) {
            String      revStr = parentNode.get("revTag").asText();

            RevisionTag parent = null;
            for(RevisionTag existing : parents) {
                if(existing.matchesPrefix(revStr)) {
                    parent = existing;
                    break;
                }
            }
            
            assertNotNull(parent);
            assertTrue(remainingParents.contains(parent));
            
            assertEquals(parent.getHeight(), parentNode.get("generation").longValue());
            remainingParents.remove(parent);
        }
        
        assertEquals(0, remainingParents.size());

        String      revStr = Util.encode64(resp.get("revTag").binaryValue());
        RevisionTag revtag = archive.getConfig().getRevisionList().tipWithPrefix(revStr);
        
        validateCanonizedRevision(revtag, base, parents);
	    return revtag;
	}
	
	public void validateCanonizedRevision(RevisionTag revtag, RevisionTag base, LinkedList<RevisionTag> parents) throws IOException {
	    long expectedHeight = 0;
	    
	    for(RevisionTag parent : parents) {
	        expectedHeight = Math.max(expectedHeight, parent.getHeight()+1);
	    }
	    
	    assertEquals(expectedHeight, revtag.getHeight());
	    
	    try(ZKFS canonFs = revtag.getFS();
	        ZKFS  baseFs = base     .getFS())
	    {
	        try(ZKDirectory canonDir = canonFs.opendir("/");
	            ZKDirectory baseDir  = baseFs .opendir("/"))
	        {
	            assertEquals(baseDir.list(), canonDir.list());
	            for(String path : canonDir.list()) {
	                assertArrayEquals(baseFs.read(path), canonFs.read(path));
	            }
	        }
	        
	        assertTrue(canonFs.getInodeTable().getStat().getMtime()
	                 > baseFs .getInodeTable().getStat().getMtime());
	        
	        RevisionInfo info = canonFs.getRevisionInfo();
	        assertEquals(info.getNumParents(), parents.size());
	        assertEquals(info.getTitle(),      baseFs.getRevisionInfo().getTitle());
	        assertEquals(info.getGeneration(), expectedHeight);
	        
	        HashSet<RevisionTag> remainingParents = new HashSet<>(parents);
	        for(RevisionTag parent : info.getParents()) {
	            assertTrue(remainingParents.contains(parent));
	            remainingParents.remove(parent);
	        }
	        
	        assertEquals(0, remainingParents.size());
	    }
	}

	@Test
	public void testGetReturnsInfoOfRequestedRevtag() throws IOException {
		JsonNode resp = WebTestUtils.requestGet(target, basePath);
		assertArrayEquals(revTag.getBytes(), resp.get("revTag").binaryValue());
		WebTestUtils.validateRevisionInfo(archive.getConfig(), resp);
	}
	
	@Test
	public void testGetReturns404IfRevtagNotFound() throws IOException {
		basePath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/revisions/doesntexist";
		WebTestUtils.requestGetWithError(target, 404, basePath);
	}
	
	@Test
	public void testCanonizeCreatesARevisionInheritingFromAllTips() throws IOException {
	    LinkedList<RevisionTag> tags = makeRevisionTriangle(4);
	    RevisionTag             base = tags.get(0);
	    String                   url = "/archives/"
	                                 + WebTestUtils.transformArchiveId(archive)
	                                 + "/revisions/"
	                                 + WebTestUtils.transformRevTag(base)
	                                 + "/canonize";
	    
	    JsonNode resp = WebTestUtils.requestPost(target, url, null);
	    validateCanonizeJson(resp, base, tags);
	}
	
	@Test
    public void testCanonizeUsesRequestedRevisionAsBase() throws IOException {
	    LinkedList<RevisionTag> tags = makeRevisionTriangle(4);
        RevisionTag             base = tags.get(1);
        String                   url = "/archives/"
                                     + WebTestUtils.transformArchiveId(archive)
                                     + "/revisions/"
                                     + WebTestUtils.transformRevTag(base)
                                     + "/canonize";
        
        JsonNode resp = WebTestUtils.requestPost(target, url, null);
        validateCanonizeJson(resp, base, tags);
    }
	
	@Test
    public void testCanonizeAcceptsActiveAsRevtag() throws IOException {
	    LinkedList<RevisionTag> tags = makeRevisionTriangle(4);
        RevisionTag             base = tags.get(1);
        State.sharedState().activeManager(archive.getConfig()).getFs().rebase(base);
        String                   url = "/archives/"
                                     + WebTestUtils.transformArchiveId(archive)
                                     + "/revisions/active/canonize";
        
        JsonNode resp = WebTestUtils.requestPost(target, url, null);
        validateCanonizeJson(resp, base, tags);
    }
	
	@Test
    public void testCanonizeSetsHeightBasedOnTallestParentWhenTallestIsBase() throws IOException {
	    LinkedList<RevisionTag> tags = makeRevisionTriangle(4);
        RevisionTag             base = tags.stream().max((a,b)->a.compareTo(b)).get();
        State.sharedState().activeManager(archive.getConfig()).getFs().rebase(base);
        String                   url = "/archives/"
                                     + WebTestUtils.transformArchiveId(archive)
                                     + "/revisions/active/canonize";
        
        JsonNode resp = WebTestUtils.requestPost(target, url, null);
        validateCanonizeJson(resp, base, tags);
    }
	
	@Test
    public void testCanonizeSetsHeightBasedOnTallestParentWhenTallestIsNotBase() throws IOException {
        LinkedList<RevisionTag> tags = makeRevisionTriangle(4);
        RevisionTag             base = tags.stream().min((a,b)->a.compareTo(b)).get();
        State.sharedState().activeManager(archive.getConfig()).getFs().rebase(base);
        String                   url = "/archives/"
                                     + WebTestUtils.transformArchiveId(archive)
                                     + "/revisions/active/canonize";
        
        JsonNode resp = WebTestUtils.requestPost(target, url, null);
        validateCanonizeJson(resp, base, tags);
    }
}
