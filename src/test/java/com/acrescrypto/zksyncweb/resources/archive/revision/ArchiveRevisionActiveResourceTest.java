package com.acrescrypto.zksyncweb.resources.archive.revision;

import static org.junit.Assert.assertArrayEquals;

import java.io.IOException;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;

import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.acrescrypto.zksyncweb.data.XRevisionInfo;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveRevisionActiveResourceTest {
    private HttpServer server;
    private WebTarget target;
    private ZKArchive archive;
    private ZKFS fs;
    private String basePath;
    
    @BeforeClass
    public static void beforeAll() {
    	ZKFSTest.cheapenArgon2Costs();
    	WebTestUtils.squelchGrizzlyLogs();
    }

    @Before
    public void beforeEach() throws Exception {
    	State.setTestState();
        server = Main.startServer();
        Client c = ClientBuilder.newClient();
        target = c.target(Main.BASE_URI);
        
    	archive = State.sharedState().getMaster().createDefaultArchive("passphrase".getBytes());
    	archive.getConfig().advertise();
    	archive.getMaster().storedAccess().storeArchiveAccess(archive.getConfig(), StoredAccess.ACCESS_LEVEL_READWRITE);
    	State.sharedState().addOpenConfig(archive.getConfig());
    	
    	fs = State.sharedState().activeFs(archive.getConfig());
    	basePath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/revisions/active";
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
    	ZKFSTest.restoreArgon2Costs();
    }
    
    @Test
    public void testGetReturnsInfoOfActiveRevtagIfActiveRevtagSetDefault() throws IOException {
    	JsonNode resp = WebTestUtils.requestGet(target, basePath);
    	RevisionTag revTag = State.sharedState().activeFs(archive.getConfig()).getBaseRevision();
    	assertArrayEquals(revTag.getBytes(), resp.get("revTag").binaryValue());
    	WebTestUtils.validateRevisionInfo(archive.getConfig(), resp);
    }
    
    @Test
    public void testGetReturnsInfoOfActiveRevtagIfActiveRevtagSetNondefault() throws IOException {
    	ZKFS fs = archive.openBlank();
    	RevisionTag revTag = fs.commit();
    	fs.commit();
    	State.sharedState().setActiveFs(archive.getConfig(), revTag.getFS());
    	
    	JsonNode resp = WebTestUtils.requestGet(target, basePath);
    	assertArrayEquals(revTag.getBytes(), resp.get("revTag").binaryValue());
    	WebTestUtils.validateRevisionInfo(archive.getConfig(), resp);
    }
    
    @Test
    public void testPutSetsActiveRevtag() throws IOException {
    	ZKFS fs = archive.openBlank();
    	RevisionTag revTag = fs.commit();
    	fs.commit();
    	State.sharedState().setActiveFs(archive.getConfig(), revTag.getFS());
    	
    	XRevisionInfo xinfo = new XRevisionInfo();
    	xinfo.setRevTag(revTag.getBytes());
    	
    	WebTestUtils.requestPut(target, basePath, xinfo);
    	assertArrayEquals(revTag.getBytes(), State.sharedState().activeFs(archive.getConfig()).getBaseRevision().getBytes());
    }
    
    @Test
    public void testDeleteClearsActiveRevtag() throws IOException {
    	ZKFS fs = archive.openBlank();
    	RevisionTag revTag = fs.commit();
    	fs.commit();
    	State.sharedState().setActiveFs(archive.getConfig(), revTag.getFS());
    	
    	WebTestUtils.requestDelete(target, basePath);
    	assertArrayEquals(archive.getConfig().getRevisionList().latest().getBytes(),
    			State.sharedState().activeFs(archive.getConfig()).getBaseRevision().getBytes());
    }
}
