package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.backedfs.BackedFS;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Shuffler;


public class ChunkAccumulatorTest {
	public class DummySwarm extends PeerSwarm {
		boolean receivedPage;
		byte[] expectedTag;
		
		public DummySwarm(byte[] expectedTag, ZKArchiveConfig config) throws IOException {
			super(config);
			this.expectedTag = expectedTag;
		}
		
		@Override
		public synchronized void receivedPage(byte[] tag) {
			if(receivedPage) throw new RuntimeException("duplicate call to receivedPage");
			assertTrue(Arrays.equals(expectedTag, tag));
			receivedPage = true;
		}
	}
	
	public class DummySocket extends PeerSocket {
		int index;
		boolean violated, closed;
		
		public DummySocket(int index, PeerSwarm swarm) {
			super(swarm);
			this.index = index;
			this.swarm = swarm;
		}
		
		@Override
		public String getAddress() {
			return "127.0.0." + index;
		}
		
		@Override public PeerAdvertisement getAd() { return null; }
		@Override public void write(byte[] data, int offset, int length) {}
		@Override public int read(byte[] data, int offset, int length) { return -1; }
		@Override public boolean isLocalRoleClient() { return false; }
		@Override public void _close() {
			closed = true;
		}
		@Override public boolean isClosed() { return closed; }
		@Override public byte[] getSharedSecret() { return null; }
		@Override public void handshake(PeerConnection conn) throws ProtocolViolationException, IOException { }
		@Override public int getPeerType() throws UnsupportedOperationException { return -1; }
		@Override
		public void violation() {
			super.violation();
			violated = true;
		}
	}
	
	public class DummyPeerConnection extends PeerConnection {
		public DummyPeerConnection(int index) throws IOException {
			super(new DummySocket(index, swarm));
		}
		
		public DummySocket getSocket() {
			return (DummySocket) socket;
		}
	}
	
	public void assertNoViolation(DummyPeerConnection conn) {
		assertFalse(conn.getSocket().violated);
		assertFalse(master.getBlacklist().contains(conn.getSocket().getAddress()));
	}
	
	public void assertViolation(DummyPeerConnection conn) {
		assertTrue(conn.getSocket().violated);
		assertTrue(master.getBlacklist().contains(conn.getSocket().getAddress()));
	}
	
	public void assertReceived() throws IOException {
		String path = Page.pathForTag(tag);
		assertTrue(swarm.receivedPage);
		assertTrue(Arrays.equals(page, localStorage.read(path)));
		assertTrue(Arrays.equals(page, archive.getStorage().read(path)));
	}
	
	public void assertNotReceived() throws IOException {
		String path = Page.pathForTag(tag);
		assertFalse(swarm.receivedPage);
		assertFalse(localStorage.exists(path));
	}
	
	static ZKMaster master;	
	static ZKArchive archive;
	static FS localStorage;
	static byte[] tag, page;
	static byte[][] chunks;
	
	static int numConnections = 16;
	ChunkAccumulator accumulator;
	DummyPeerConnection[] connections;
	DummySwarm swarm;
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		
		try(ZKFS fs = archive.openBlank()) {
			fs.write("file", new byte[archive.getConfig().getPageSize()]);
			fs.commit();
			PageTree tree = new PageTree(fs.inodeForPath("file"));
			tag = tree.getPageTag(0);
			page = archive.getStorage().read(Page.pathForTag(tag));
			
			ByteBuffer buf = ByteBuffer.wrap(page);
			int i = 0;
			chunks = new byte[(int) Math.ceil((double) page.length/PeerMessage.FILE_CHUNK_SIZE)][];
			while(buf.hasRemaining()) {
				int size = Math.min(PeerMessage.FILE_CHUNK_SIZE, buf.remaining());
				chunks[i] = new byte[size];
				buf.get(chunks[i]);
				i++;
			}
			
			archive.getStorage().purge();
			localStorage = ((BackedFS) archive.getStorage()).getCacheFS();
		}
	}
	
	@Before
	public void beforeEach() throws IOException {
		swarm = new DummySwarm(tag, archive.getConfig());
		connections = new DummyPeerConnection[numConnections];
		for(int i = 0; i < connections.length; i++) {
			connections[i] = new DummyPeerConnection(i);
		}
		accumulator = new ChunkAccumulator(swarm, tag, chunks.length);
	}
	
	@After
	public void afterEach() throws IOException {
		for(DummyPeerConnection conn : connections) {
			conn.close();
		}
		
		swarm.close();
		master.scratchStorage().purge();
		archive.getStorage().purge();
	}
	
	@AfterClass
	public static void afterAll() throws IOException {
		archive.close();
		master.close();
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	@Test
	public void testBlank() {}
	
	@Test
	public void testAddChunkReturnsTrueIfAndOnlyIfFileFinished() throws IOException {
		for(int i = 0; i < chunks.length; i++) {
			boolean last = i == chunks.length - 1;
			assertEquals(last, accumulator.addChunk(i, chunks[i], connections[0]));
		}
	}
	
	@Test
	public void testAddChunkCallsReceivedPageWhenFinished() throws IOException {
		assertFalse(swarm.receivedPage);
		
		for(int i = 0; i < chunks.length; i++) {
			accumulator.addChunk(i, chunks[i], connections[0]);
		}
		
		assertTrue(swarm.receivedPage);
	}
	
	@Test
	public void testAddChunkDoesntCallReceivedPageWhenPageDoesntValidate() throws IOException {
		assertFalse(swarm.receivedPage);
		
		for(int i = 0; i < chunks.length; i++) {
			byte[] chunk = chunks[i];
			if(i == 1) {
				chunk = chunk.clone();
				chunk[0] ^= 0x01;
			}
			accumulator.addChunk(i, chunk, connections[0]);
		}
		
		assertNotReceived();
	}
	
	@Test
	public void testAddChunkWritesCompletedFile() throws IOException {
		String path = Page.pathForTag(tag);
		assertFalse(localStorage.exists(path));
		
		for(int i = 0; i < chunks.length; i++) {
			boolean shouldExist = accumulator.addChunk(i, chunks[i], connections[0]);
			assertEquals(shouldExist, localStorage.exists(path));
		}		

		assertTrue(Arrays.equals(page, localStorage.read(path)));
		assertTrue(Arrays.equals(page, archive.getStorage().read(path)));
	}
	
	@Test
	public void testIdentifiesCorrectVersions() throws IOException {
		for(int i = 0; i < chunks.length; i++) {
			if(i == 1) {
				byte[] invalidChunk = chunks[i].clone();
				invalidChunk[0] ^= 0x01;
				accumulator.addChunk(i, invalidChunk, connections[1]);
			}
			accumulator.addChunk(i, chunks[i], connections[0]);
		}
		
		assertReceived();
	}
	
	@Test
	public void testBlacklistsPeersSendingBadChunks() throws IOException {
		for(int i = 0; i < chunks.length; i++) {
			if(i == 1) {
				byte[] invalidChunk = chunks[i].clone();
				invalidChunk[0] ^= 0x01;
				accumulator.addChunk(i, invalidChunk, connections[1]);
			}
			accumulator.addChunk(i, chunks[i], connections[0]);
		}
		
		assertNoViolation(connections[0]);
		assertViolation(connections[1]);
	}
	
	@Test
	public void testAcceptsPagesInRandomOrder() throws IOException {
		Shuffler shuffler = new Shuffler(chunks.length);
		while(shuffler.hasNext()) {
			int idx = shuffler.next();
			accumulator.addChunk(idx, chunks[idx], connections[0]);
		}
		
		assertReceived();
	}
	
	@Test
	public void testConcurrency() throws IOException {
		Thread[] threads = new Thread[numConnections];
		for(int i = 0; i < numConnections; i++) {
			final int threadId = i;
			threads[i] = new Thread(()-> {
				Shuffler shuffler = new Shuffler(chunks.length);
				while(shuffler.hasNext()) {
					int idx = shuffler.next();
					try {
						accumulator.addChunk(idx, chunks[idx], connections[threadId]);
					} catch (IOException e) {
						e.printStackTrace();
						fail();
					}
				}
			});
			threads[i].start();
		}
		
		for(Thread thread : threads) {
			while(thread.isAlive()) {
				try {
					thread.join();
				} catch (InterruptedException e) {
				}
			}
		}
		
		assertReceived();
	}
	
	@Test
	public void testAddChunkValidatesIndexMin() throws IOException {
		try {
			accumulator.addChunk(-1, chunks[0], connections[0]);
			fail();
		} catch(EINVALException exc) {
		}
	}

	@Test
	public void testAddChunkValidatesIndexMax() throws IOException {
		try {
			accumulator.addChunk(chunks.length, chunks[0], connections[0]);
			fail();
		} catch(EINVALException exc) {
		}
	}
	
	@Test
	public void testAssemblesChunksFromMultiplePeers() throws IOException {
		for(int i = 0; i < chunks.length; i++) {
			accumulator.addChunk(i, chunks[i], connections[i % connections.length]);
		}
		
		assertReceived();
	}
	
	@Test
	public void testValidatesTag() throws IOException {
		accumulator.tag[0] ^= 0x01;

		for(int i = 0; i < chunks.length; i++) {
			accumulator.addChunk(i, chunks[i], connections[i % connections.length]);
		}
		
		assertNotReceived();
	}
	
	@Test
	public void testIsFinishedReturnsFalseIfFileHasNotBeenCompleted() {
		assertFalse(accumulator.isFinished());
	}
	
	@Test
	public void testIsFinishedReturnsTrueIfFileHasBeenCompleted() throws IOException {
		for(int i = 0; i < chunks.length; i++) {
			boolean last = i == chunks.length - 1;
			assertFalse(accumulator.isFinished());
			assertEquals(last, accumulator.addChunk(i, chunks[i], connections[0]));
		}
		
		assertTrue(accumulator.isFinished());
	}
}
