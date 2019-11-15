package com.acrescrypto.zksyncweb.resources.archive;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;

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
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.StoredAccess;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.net.PageQueue;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.Main;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebTestUtils;
import com.fasterxml.jackson.databind.JsonNode;

public class ArchiveFsResourceTest {
	private HttpServer server;
	private WebTarget target;
	private ZKArchive archive;
	private ZKFS fs;
	private String basePath;

	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
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
		basePath = "/archives/" + WebTestUtils.transformArchiveId(archive) + "/fs/";
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
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}

	long setupMissingFile() throws IOException {
		fs.write("missing", new byte[archive.getConfig().getPageSize()]);
		fs.commit();
		PageTree tree = new PageTree(fs.inodeForPath("missing"));
		fs.getArchive().getConfig().getCacheStorage().unlink(tree.getRefTag().getStorageTag().path());
		return tree.getInodeId();
	}

	long setupMissingDirectory() throws IOException {
		fs.mkdir("missing");
		for(int i = 0; i < 100; i++) {
			fs.write("missing/" + Util.bytesToHex(archive.getCrypto().symNonce(i)), new byte[0]);
		}

		fs.commit();
		PageTree tree = new PageTree(fs.inodeForPath("missing"));
		fs.getArchive().getConfig().getCacheStorage().unlink(tree.getRefTag().getStorageTag().path());
		fs.uncache("/missing");
		return tree.getInodeId();
	}

	@Test
	public void testPostSetsFileContents() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		WebTestUtils.requestBinaryPost(target, basePath + "file", contents);
		assertArrayEquals(contents, fs.read("file"));
	}

	@Test
	public void testPostOverwritesExistingFiles() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		fs.write("path/to/file", new byte[65536]);
		WebTestUtils.requestBinaryPost(target, basePath + "path/to/file", contents);
		assertArrayEquals(contents, fs.read("path/to/file"));
	}

	@Test
	public void testPostWritesAtRequestedOffset() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		int offset = 1234;
		byte[] fullContents = new byte[offset + contents.length];
		System.arraycopy(contents, 0, fullContents, offset, contents.length);

		fs.write("path/to/file", new byte[65536]);
		WebTestUtils.requestBinaryPost(target, basePath + "path/to/file?offset=" + offset, contents);
		assertArrayEquals(fullContents, fs.read("path/to/file"));
	}

	@Test
	public void testPostWritesWithoutTruncationIfRequested() throws IOException {
		fs.write("path/to/file", "cats are great".getBytes());
		WebTestUtils.requestBinaryPost(target, basePath + "path/to/file?truncate=false", "dogs".getBytes());
		assertArrayEquals("dogs are great".getBytes(), fs.read("path/to/file"));
	}

	@Test
	public void testPostWritesAtSpecifiedOffsetWithoutTruncationIfRequested() throws IOException {
		int size = 65536;
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		int offset = 1234;
		byte[] fullContents = new byte[size];
		System.arraycopy(contents, 0, fullContents, offset, contents.length);

		fs.write("path/to/file", new byte[size]);
		WebTestUtils.requestBinaryPost(target, basePath + "path/to/file?offset=" + offset + "&truncate=false", contents);
		assertArrayEquals(fullContents, fs.read("path/to/file"));
	}

	@Test
	public void testPostReturns409IfPathIsDirectoryAndContentsAreSpecified() throws IOException {
		fs.mkdir("dir");
		WebTestUtils.requestBinaryPostWithError(target, 409, basePath + "dir", "hi".getBytes());
	}

	@Test
	public void testPostReturns409IfPathIsDirectoryAndTruncateNotDisabled() throws IOException {
		fs.mkdir("dir");
		WebTestUtils.requestBinaryPostWithError(target, 409, basePath + "dir", new byte[0]);
	}

	@Test
	public void testPostReturns409IfPathIsDirectoryAndNonzeroOffsetSpecified() throws IOException {
		fs.mkdir("dir");
		WebTestUtils.requestBinaryPostWithError(target, 409, basePath + "dir?offset=1", new byte[0]);
	}

	@Test
	public void testPostSucceedsIfPathIsDirectoryWithZeroOffsetTruncateFalseNoData() throws IOException {
		fs.mkdir("dir");
		WebTestUtils.requestBinaryPost(target,  basePath + "dir?truncate=false", new byte[0]);
	}

	@Test
	public void testPostAllowsChangeUid() throws IOException {
		fs.write("file", new byte[0]);
		WebTestUtils.requestBinaryPost(target,  basePath + "file?uid=1234", new byte[0]);
		assertEquals(1234, fs.stat("file").getUid());
	}

	@Test
	public void testPostDoesNotModifyUidUnlessRequested() throws IOException {
		fs.write("file", new byte[0]);
		fs.chown("file", 1111);
		WebTestUtils.requestBinaryPost(target,  basePath + "file", new byte[0]);
		assertEquals(1111, fs.stat("file").getUid());
	}

	@Test
	public void testPostAllowsChangeUsername() throws IOException {
		fs.write("file", new byte[0]);
		WebTestUtils.requestBinaryPost(target,  basePath + "file?user=nickcave", new byte[0]);
		assertEquals("nickcave", fs.stat("file").getUser());
	}

	@Test
	public void testPostDoesNotModifyUserUnlessRequested() throws IOException {
		fs.write("file", new byte[0]);
		fs.chown("file", "1111");
		WebTestUtils.requestBinaryPost(target,  basePath + "file", new byte[0]);
		assertEquals("1111", fs.stat("file").getUser());
	}

	@Test
	public void testPostAllowsChangeGid() throws IOException {
		fs.write("file", new byte[0]);
		WebTestUtils.requestBinaryPost(target,  basePath + "file?gid=4321", new byte[0]);
		assertEquals(4321, fs.stat("file").getGid());
	}

	@Test
	public void testPostDoesNotModifyGidUnlessRequested() throws IOException {
		fs.write("file", new byte[0]);
		fs.chgrp("file", 1111);
		WebTestUtils.requestBinaryPost(target,  basePath + "file", new byte[0]);
		assertEquals(1111, fs.stat("file").getGid());
	}

	@Test
	public void testPostAllowsChangeGroup() throws IOException {
		fs.write("file", new byte[0]);
		WebTestUtils.requestBinaryPost(target,  basePath + "file?group=badseeds", new byte[0]);
		assertEquals("badseeds", fs.stat("file").getGroup());
	}

	@Test
	public void testPostDoesNotModifyGroupUnlessRequested() throws IOException {
		fs.write("file", new byte[0]);
		fs.chgrp("file", "1111");
		WebTestUtils.requestBinaryPost(target,  basePath + "file", new byte[0]);
		assertEquals("1111", fs.stat("file").getGroup());
	}

	@Test
	public void testPostAllowsChangeModeOctal() throws IOException {
		fs.write("file", new byte[0]);
		WebTestUtils.requestBinaryPost(target,  basePath + "file?mode=0734", new byte[0]);
		assertEquals(0734, fs.stat("file").getMode());
	}

	@Test
	public void testPostAllowsChangeModeDecimal() throws IOException {
		fs.write("file", new byte[0]);
		WebTestUtils.requestBinaryPost(target,  basePath + "file?mode=1234", new byte[0]);
		assertEquals(1234, fs.stat("file").getMode());
	}

	@Test
	public void testPostDoesNotModifyModeUnlessRequested() throws IOException {
		fs.write("file", new byte[0]);
		fs.chmod("file", 0777);
		WebTestUtils.requestBinaryPost(target,  basePath + "file", new byte[0]);
		assertEquals(0777, fs.stat("file").getMode());
	}

	@Test
	public void testGetReturnsFileContents() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		fs.write("path/to/file", contents);
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "path/to/file");
		assertArrayEquals(contents, response);
	}

	@Test
	public void testGetReturnsStatIfRequested() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		fs.write("path/to/file", contents);
		JsonNode response = WebTestUtils.requestGet(target, basePath + "path/to/file?stat=true");
		WebTestUtils.validatePathStat(fs, "/", response);
	}

	@Test
	public void testGetReturnsFileBeginningAtRequestedOffset() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		int offset = 16;
		fs.write("path/to/file", contents);
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "path/to/file?offset=" + offset);
		byte[] truncation = new byte[contents.length - offset];
		System.arraycopy(contents, offset, truncation, 0, contents.length-offset);
		assertArrayEquals(truncation, response);
	}

	@Test
	public void testGetReturnsFileTruncatedToRequestedLength() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		int length = 16;
		fs.write("path/to/file", contents);
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "path/to/file?length=" + length);
		byte[] truncation = new byte[length];
		System.arraycopy(contents, 0, truncation, 0, length);
		assertArrayEquals(truncation, response);
	}

	@Test
	public void testGetReturnsFileTruncatedToRequestedLengthWithRequestedOffset() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		int length = 13, offset = 20;
		fs.write("path/to/file", contents);
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "path/to/file?length=" + length + "&offset=" + offset);
		byte[] truncation = new byte[length];
		System.arraycopy(contents, offset, truncation, 0, length);
		assertArrayEquals(truncation, response);
	}

	@Test
	public void testGetReturnsFileToEOFIfLengthGoesBeyondEOF() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		fs.write("path/to/file", contents);
		byte[] response = WebTestUtils.requestBinaryGet(target, basePath + "path/to/file?length=" + 1024);
		assertArrayEquals(contents, response);
	}

	@Test
	public void testGetReturns404ForNonexistentPath() throws IOException {
		WebTestUtils.requestGetWithError(target, 404, basePath + "path/to/file");
	}

	@Test
	public void testGetStatDoesNotEnqueueFile() throws IOException {
		long inodeId = setupMissingFile();
		WebTestUtils.requestGet(target, basePath + "missing?stat=true");
		assertEquals(PageQueue.CANCEL_PRIORITY,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetStatDoesNotModifyPriority() throws IOException {
		long inodeId = setupMissingFile();
		archive.getConfig().getSwarm().requestInode(0, fs.getBaseRevision(), inodeId);
		WebTestUtils.requestGet(target, basePath + "missing?stat=true&priority=10");
		assertEquals(0,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetStatFileShowsCurrentPriorityIfEnqueued() throws IOException {
		long inodeId = setupMissingFile();
		archive.getConfig().getSwarm().requestInode(10, fs.getBaseRevision(), inodeId);
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "missing?stat=true");
		assertTrue(resp.get("isRequested").asBoolean());
		assertEquals(10, resp.get("priority").asInt());
	}

	@Test
	public void testGetStatFileShowsCancelPriorityIfEnqueued() throws IOException {
		setupMissingFile();
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "missing?stat=true");
		assertFalse(resp.get("isRequested").asBoolean());
		assertEquals(PageQueue.CANCEL_PRIORITY, resp.get("priority").asInt());
	}

	@Test
	public void testGetStatDirectoryShowsCurrentPriorityIfEnqueued() throws IOException {
		long inodeId = setupMissingDirectory();
		archive.getConfig().getSwarm().requestInode(10, fs.getBaseRevision(), inodeId);
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "missing?stat=true");
		assertTrue(resp.get("isRequested").asBoolean());
		assertEquals(10, resp.get("priority").asInt());
	}

	@Test
	public void testGetStatDirectoryShowsCancelPriorityIfEnqueued() throws IOException {
		setupMissingDirectory();
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "missing?stat=true");
		assertFalse(resp.get("isRequested").asBoolean());
		assertEquals(PageQueue.CANCEL_PRIORITY, resp.get("priority").asInt());
	}

	@Test
	public void testGetQueueAllowsEnqueueOfFile() throws IOException {
		long inodeId = setupMissingFile();
		WebTestUtils.requestGet(target, basePath + "missing?queue=true");
		assertEquals(0,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetQueueAllowsEnqueueOfDirectory() throws IOException {
		long inodeId = setupMissingDirectory();
		WebTestUtils.requestGet(target, basePath + "missing?queue=true");
		assertEquals(0,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetQueueAllowsEnqueueOfFileWithNondefaultPriority() throws IOException {
		long inodeId = setupMissingFile();
		WebTestUtils.requestGet(target, basePath + "missing?queue=true&priority=17");
		assertEquals(17,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetQueueAllowsEnqueueOfDirectoryWithNondefaultPriority() throws IOException {
		long inodeId = setupMissingDirectory();
		WebTestUtils.requestGet(target, basePath + "missing?queue=true&priority=17");
		assertEquals(17,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetQueueDoesNotOverwritePriorityIfFileAlreadyEnqueuedAndNoPrioritySpecified() throws IOException {
		long inodeId = setupMissingFile();
		archive.getConfig().getSwarm().requestInode(12, fs.getBaseRevision(), inodeId);
		WebTestUtils.requestGet(target, basePath + "missing?queue=true");
		assertEquals(12,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetQueueDoesNotOverwritePriorityIfDirectoryAlreadyEnqueuedAndNoPrioritySpecified() throws IOException {
		long inodeId = setupMissingDirectory();
		archive.getConfig().getSwarm().requestInode(12, fs.getBaseRevision(), inodeId);
		WebTestUtils.requestGet(target, basePath + "missing?queue=true");
		assertEquals(12,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetQueueDoesOverwritesPriorityIfFileAlreadyEnqueuedAndPrioritySpecified() throws IOException {
		long inodeId = setupMissingFile();
		archive.getConfig().getSwarm().requestInode(12, fs.getBaseRevision(), inodeId);
		WebTestUtils.requestGet(target, basePath + "missing?queue=true&priority=13");
		assertEquals(13,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetQueueDoesOverwritesPriorityIfDirectoryAlreadyEnqueuedAndPrioritySpecified() throws IOException {
		long inodeId = setupMissingDirectory();
		archive.getConfig().getSwarm().requestInode(12, fs.getBaseRevision(), inodeId);
		WebTestUtils.requestGet(target, basePath + "missing?queue=true&priority=13");
		assertEquals(13,
				archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId));
	}

	@Test
	public void testGetSetsDefaultPriorityInReadFile() throws IOException {
		long inodeId = setupMissingFile();
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing");
			} catch(Exception exc) {} // our request hangs since the path is missing, so squelch the error
		}).start();
		assertTrue(Util.waitUntil(2000,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == 0));
	}

	@Test
	public void testGetSetsDefaultPriorityInReadDirectory() throws IOException {
		long inodeId = setupMissingDirectory();
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing");
			} catch(Exception exc) {} // our request hangs since the path is missing, so squelch the error
		}).start();
		assertTrue(Util.waitUntil(2000,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == 0));
	}

	@Test
	public void testGetAllowsSettingOfPriorityInReadFile() throws IOException {
		long inodeId = setupMissingFile();
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing?priority=13");
			} catch(Exception exc) {}
		}).start();
		assertTrue(Util.waitUntil(2000,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == 13));
	}

	@Test
	public void testGetAllowsSettingOfPriorityInReadDirectory() throws IOException {
		long inodeId = setupMissingDirectory();
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing?priority=13");
			} catch(Exception exc) {}
		}).start();
		assertTrue(Util.waitUntil(2000,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == 13));
	}

	@Test
	public void testGetDoesNotOverwritePriorityInReadFileIfPriorityNotSpecified() throws IOException {
		long inodeId = setupMissingFile();
		archive.getConfig().getSwarm().requestInode(-1234, fs.getBaseRevision(), inodeId);
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing");
			} catch(Exception exc) {}
		}).start();
		assertFalse(Util.waitUntil(500,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) != -1234));
	}

	@Test
	public void testGetDoesNotOverwritePriorityInReadDirectoryIfPriorityNotSpecified() throws IOException {
		long inodeId = setupMissingDirectory();
		archive.getConfig().getSwarm().requestInode(-1234, fs.getBaseRevision(), inodeId);
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing");
			} catch(Exception exc) {}
		}).start();
		assertFalse(Util.waitUntil(500,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) != -1234));
	}

	@Test
	public void testGetOverwritesPriorityInReadFileIfPrioritySpecified() throws IOException {
		long inodeId = setupMissingFile();
		archive.getConfig().getSwarm().requestInode(4321, fs.getBaseRevision(), inodeId);
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing?priority=-1234");
			} catch(Exception exc) {}
		}).start();
		assertTrue(Util.waitUntil(500,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == -1234));
	}

	@Test
	public void testGetOverwritesPriorityInReadDirectoryIfPrioritySpecified() throws IOException {
		long inodeId = setupMissingDirectory();
		archive.getConfig().getSwarm().requestInode(4321, fs.getBaseRevision(), inodeId);
		new Thread(()->{
			try {
				WebTestUtils.requestGet(target, basePath + "missing?priority=-1234");
			} catch(Exception exc) {}
		}).start();
		assertTrue(Util.waitUntil(500,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == -1234));
	}

	@Test
	public void testGetCancelRemovesFileFromQueue() throws IOException {
		long inodeId = setupMissingFile();
		archive.getConfig().getSwarm().requestInode(4321, fs.getBaseRevision(), inodeId);
		WebTestUtils.requestGet(target, basePath + "missing?cancel=true");
		assertTrue(Util.waitUntil(500,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == PageQueue.CANCEL_PRIORITY));
	}

	@Test
	public void testGetCancelRemovesDirectoryFromQueue() throws IOException {
		long inodeId = setupMissingDirectory();
		archive.getConfig().getSwarm().requestInode(4321, fs.getBaseRevision(), inodeId);
		WebTestUtils.requestGet(target, basePath + "missing?cancel=true");
		assertTrue(Util.waitUntil(500,
				()->archive.getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId) == PageQueue.CANCEL_PRIORITY));
	}

	@Test
	public void testGetReturnsListingIfPathIsDirectory() throws IOException {
		fs.mkdir("dir");
		fs.write("dir/1", "1".getBytes());
		fs.write("dir/2", "2".getBytes());
		fs.write("dir/3", "3".getBytes());

		JsonNode resp = WebTestUtils.requestGet(target, basePath + "dir");
		assertTrue(resp.get("entries").isArray());
		assertEquals(4, resp.get("entries").size());

		ArrayList<String> items = new ArrayList<>();
		resp.get("entries").forEach((node)->items.add(node.asText()));

		assertTrue(items.contains("/dir/1"));
		assertTrue(items.contains("/dir/2"));
		assertTrue(items.contains("/dir/3"));
	}

	@Test
	public void testGetReturnsStatIfPathIsDirectoryIfStatIsDirectory() throws IOException {
		fs.mkdir("dir");
		JsonNode resp = WebTestUtils.requestGet(target, basePath + "dir/?stat=true");

		try {
			WebTestUtils.validatePathStat(fs, "", resp);
		} catch (IOException e) {
			fail();
		}
	}

	@Test
	public void testGetReturnsStatListingIfPathIsDirectoryAndListStatRequested() throws IOException {
		fs.mkdir("dir");
		fs.write("dir/1", "1".getBytes());
		fs.write("dir/2", new byte[123]);
		fs.write("dir/3", new byte[1024]);
		fs.mknod("dir/blockdev", Stat.TYPE_BLOCK_DEVICE, 1, 2);
		fs.mknod("dir/chardev", Stat.TYPE_CHARACTER_DEVICE, 1, 2);
		fs.mkfifo("dir/fifo");
		fs.mkdir("dir/dir2");
		fs.link("dir/1", "dir/1link");
		fs.symlink("dir/2", "dir/2symlink");

		JsonNode resp = WebTestUtils.requestGet(target, basePath + "dir/?liststat=true");
		assertTrue(resp.get("entries").isArray());
		assertEquals(10, resp.get("entries").size());

		resp.get("entries").forEach((node)->{
			try {
				WebTestUtils.validatePathStat(fs, "/", node);
			} catch (IOException e) {
				fail();
			}
		});
	}

	@Test
	public void testGetReturnsRecursiveListingIfPathIsDirectoryAndRecursiveSetTrue() throws IOException {
		fs.mkdir("dir");
		fs.write("dir/a/1", "1".getBytes());
		fs.write("dir/b/2", "2".getBytes());
		fs.write("dir/c/3", "3".getBytes());

		JsonNode resp = WebTestUtils.requestGet(target, basePath + "dir?recursive=true");
		assertTrue(resp.get("entries").isArray());
		assertEquals(7, resp.get("entries").size());

		ArrayList<String> items = new ArrayList<>();
		resp.get("entries").forEach((node)->items.add(node.asText()));
		
		assertTrue(items.contains("/dir/a"));
		assertTrue(items.contains("/dir/b"));
		assertTrue(items.contains("/dir/c"));
		assertTrue(items.contains("/dir/a/1"));
		assertTrue(items.contains("/dir/b/2"));
		assertTrue(items.contains("/dir/c/3"));
	}

	@Test
	public void testGetRootListsRootDirectory() throws IOException {
		fs.write("1", "1".getBytes());

		JsonNode resp = WebTestUtils.requestGet(target, basePath);
		assertTrue(resp.get("entries").isArray());
		assertEquals(2, resp.get("entries").size());

		ArrayList<String> items = new ArrayList<>();
		resp.get("entries").forEach((node)->items.add(node.asText()));

		assertTrue(items.contains("/1"));
	}

	@Test
	public void testDeleteUnlinksFile() throws IOException {
		byte[] contents = State.sharedCrypto().hash(new byte[0]);
		fs.write("path/to/file", contents);
		WebTestUtils.requestDelete(target, basePath + "path/to/file");
		assertFalse(fs.exists("path/to/file"));
	}

	@Test
	public void testDeleteReturns404ForNonexistentPath() throws IOException {
		WebTestUtils.requestDeleteWithError(target, 404, basePath + "path/to/file");
	}

	@Test
	public void testDeleteUnlinksDirectories() throws IOException {
		fs.mkdir("dir");
		WebTestUtils.requestDelete(target, basePath + "dir");
		assertFalse(fs.exists("dir"));
	}

	@Test
	public void testDeleteReturns409IfUnlinkingNonemptyDirectory() throws IOException {
		fs.mkdir("dir");
		fs.write("dir/file", new byte[0]);
		WebTestUtils.requestDeleteWithError(target, 409, basePath + "dir");
		assertTrue(fs.exists("dir"));
	}
}
