package com.acrescrypto.zksync.complextests;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFSManager;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.utility.FSTestWriter;
import com.acrescrypto.zksync.utility.Util;

public class TwoPeersOneWayComplexTest {
	
	public class PathMismatchException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		String path, reason;
		public PathMismatchException(String path, String reason) {
			super("Path mismatch: " + path + " -- " + reason);
			this.path = path;
			this.reason = reason;
		}
	}
	
	public final static String basePath = "/tmp/zksync-test/twopeersoneway/";
	
	public ZKMaster dhtRoot;
	public ZKFSManager alice, bob;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() throws IOException {
		scrub();
		dhtRoot = initDHTRoot();
		alice = initAlice();
		bob = initBob();
	}
	
	@After
	public void afterEach() throws IOException {
		dhtRoot.close();
		alice.close();
		bob.close();
		// scrub();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	public void scrub() throws IOException {
		System.out.println("Scrubbing " + basePath);
		try(LocalFS fs = new LocalFS("/")) {
			if(fs.exists(basePath)) {
				fs.rmrf(basePath);
			}
		}
	}
	
	public String automirrorPath(String peerName) {
		return basePath + peerName + "/fs";
	}
	
	public String encryptedDataPath(String peerName) {
		return basePath + peerName + "/data";
	}
	
	public ZKMaster initDHTRoot() throws IOException {
		ZKMaster root = ZKMaster.openBlankTestVolume("dht");
		root.getDHTClient().listen("127.0.0.1", 0);
		assertTrue(Util.waitUntil(50, ()->root.getDHTClient().getStatus() >= DHTClient.STATUS_QUESTIONABLE));
		return root;
	}
	
	public ZKArchiveConfig initConfig(String peerName) throws IOException {
		ZKMaster master = ZKMaster.open(ZKMaster.demoPassphraseProvider(),
				new LocalFS(encryptedDataPath(peerName)));
		master.getGlobalConfig().set("net.swarm.enabled", true);
		master.activateDHT("127.0.0.1", 0, dhtRoot.getDHTClient().getLocalPeer());
		ZKArchive archive = master.createDefaultArchive("TwoPeersOneWay".getBytes());
		archive.getConfig().advertise();
		return archive.getConfig();
	}
	
	public ZKFSManager initPeer(String peerName) throws IOException {
		try(LocalFS fs = new LocalFS("/")) {
			fs.mkdirp(automirrorPath(peerName));
			fs.mkdirp(encryptedDataPath(peerName));
		}

		ZKArchiveConfig config = initConfig(peerName);
		ZKFSManager manager = new ZKFSManager(config);
		
		manager.setAutofollow(true);
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(1000);
		manager.setAutomirrorPath(automirrorPath(peerName));
		manager.setAutomirror(true);;
		manager.setAutomerge(true);
		
		return manager;
	}

	public ZKFSManager initAlice() throws IOException {
		ZKFSManager manager = initPeer("alice");
		return manager;
	}
	
	public ZKFSManager initBob() throws IOException {
		ZKFSManager manager = initPeer("bob");
		return manager;
	}
	
	public int numConnections(ZKFSManager manager) {
		return manager.getFs().getArchive().getConfig().getSwarm().numConnections();
	}
	
	public boolean fsMatch(FS ref, FS comp) throws IOException {
		MutableBoolean matches = new MutableBoolean();
		matches.setTrue();
		
		try(Directory dir = ref.opendir("/")) {
			dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBroken)->{
				try {
					Stat otherStat = comp.lstat(path);
					if(otherStat.getSize() != stat.getSize()) throw new PathMismatchException(path, "Mismatched size " + stat.getSize() + " vs " + otherStat.getSize());
					if(otherStat.getMode() != stat.getMode()) throw new PathMismatchException(path, "Mismatched mode");
					if(otherStat.getType() != stat.getType()) throw new PathMismatchException(path, "Mismatched type");
					if(stat.isSymlink()) {
						if(!ref.readlink(path).equals(comp.readlink(path))) {
							 throw new PathMismatchException(path, "Mismatched link targets");
						}
					} else if(stat.isRegularFile()) {
						if(!Arrays.equals(ref.read(path), comp.read(path))) {
							 throw new PathMismatchException(path, "Mismatched contents");
						}
					}
				} catch(PathMismatchException exc) {
					// TODO: ditch this and matches when done
					System.out.println("Path mismatch: " + exc.path + " " + exc.reason);
					matches.setFalse();
				}
			});
			return matches.booleanValue();
		} catch(PathMismatchException exc) {
			System.out.println("Path mismatch: " + exc.path + " " + exc.reason);
			return false;
		}
	}
	
	public boolean peersMatch() throws IOException {
		if(!alice.getFs().getBaseRevision().equals(bob.getFs().getBaseRevision())) {
			System.out.println("Mismatched revisions");
			System.out.println("\tAlice: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
			System.out.println("\t  Bob: " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
			
			System.out.println("\tTesting alice -> bob...");
			if(fsMatch(alice.getMirror().getTarget(), bob.getMirror().getTarget())) {
				System.out.println("\t\talice -> bob matches");
			}
			
			System.out.println("\tTesting bob -> alice...");
			if(fsMatch(bob.getMirror().getTarget(), alice.getMirror().getTarget())) {
				System.out.println("\t\tbob -> alice matches");
			}
			
			System.out.println("\tTesting ZKFS alice -> ZKFS bob...");
			if(fsMatch(alice.getFs(), bob.getFs())) {
				System.out.println("\t\talice -> bob matches");
			}
			
			System.out.println("\tTesting ZKFS bob -> ZKFS alice...");
			if(fsMatch(bob.getFs(), alice.getFs())) {
				System.out.println("\t\tbob -> alice matches");
			}
			
			return false;
		}
		
		if(!fsMatch(alice.getMirror().getTarget(), bob.getMirror().getTarget())) return false;
		if(!fsMatch(bob.getMirror().getTarget(), alice.getMirror().getTarget())) return false;
		return true;
	}
	
	public void assertPeersMatch() {
		long startTime = System.currentTimeMillis();
		assertTrue(Util.waitUntil(60000, ()->{
			try {
				return peersMatch();
			} catch(ENOENTException exc) {
				exc.printStackTrace();
				return false;
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
				return false;
			}
		}));
		System.out.println("Match found in " + (System.currentTimeMillis() - startTime));
	}

	/** This is an equivalent to the "two-peer one-way" live test, in which we have
	 * a single writer (Alice) and a single reader (Bob). They discover one another
	 * via the DHT (using a simulated central DHT node that does not participate in the
	 * archive), and synchronize an ongoing set of simple filesystem revisions discovered
	 * via Alice's FSMirror. Bob also uses FSMirror to synchronize data to disk.
	 * @throws IOException 
	 */
	@Test
	public void testSimpleTwoPeerEquivalent() throws IOException {
		FSTestWriter writer = new FSTestWriter(alice.getMirror().getTarget(), 2);
		long writeIntervalMs = 1500;
		
		assertTrue(Util.waitUntil(1000, ()->numConnections(alice) > 0));
		assertTrue(Util.waitUntil(1000, ()->numConnections(bob) > 0));
		
		System.out.println("Test ready.");
		long startTime = System.currentTimeMillis();
		long operations = 0;
		
		while(true) {
			if(operations++ % 10 == 0) {
				long elapsed = (System.currentTimeMillis() - startTime)/1000;
				System.out.println("Time: " + new java.util.Date());
				System.out.println("Alice revision: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
				System.out.println("  Bob revision: " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
				System.out.println(operations + " operations complete, running " + elapsed + "s");
				try {
					assertPeersMatch();
				} catch(Throwable exc) {
					System.out.println("Peers do not match");
					exc.printStackTrace();
					throw exc;
				}
				System.out.println("Peers match\n");
			}
			
			writer.act();
			Util.sleep(writeIntervalMs);
		}
	}
}
