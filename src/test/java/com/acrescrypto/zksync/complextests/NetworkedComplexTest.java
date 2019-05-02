package com.acrescrypto.zksync.complextests;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

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
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSManager;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.utility.FSTestWriter;
import com.acrescrypto.zksync.utility.Util;

public class NetworkedComplexTest {
	
	public class PathMismatchException extends RuntimeException {
		private static final long serialVersionUID = 1L;
		String path, reason;
		public PathMismatchException(String path, String reason) {
			super("Path mismatch: " + path + " -- " + reason);
			this.path = path;
			this.reason = reason;
		}
	}
	
	public final static String basePath = "/tmp/zksync-test/networkedcomplex/";
	
	public ZKMaster dhtRoot;
	public ZKFSManager alice, bob, charlie;
	
	String lastLog = null;
	int lastLogRepeatCount = 0;
	
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
		if(charlie != null) {
			charlie.close();
		}
		// scrub();
	}
	
	@AfterClass
	public static void afterAll() {
		// TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	public void log(String str) {
		if(lastLog != null && str.equals(lastLog)) {
			// don't spam the console with repeated messages
			/* print a tilde for each repeat, and then to stop tilde spam, only print
			/* one tilde for every 10 repeats after the first 10, then only 1 per 100 after the first
			 * 100, and so on. 
			 */
			lastLogRepeatCount++;
			
			double l10 = Math.floor(Math.log10(lastLogRepeatCount));
			double l10p = Math.floor(Math.log10(lastLogRepeatCount-1));
			if(l10 != l10p && l10 != 0) System.out.println();
			double power = Math.round(Math.pow(10, l10));
			
			if(lastLogRepeatCount % power == 0) {
				System.out.print("~");
			}
			return;
		}
		
		if(lastLogRepeatCount > 0) {
			System.out.println();
		}
		
		System.out.print(new Date());
		System.out.print(" ");
		System.out.println(str);
		lastLog = str;
		lastLogRepeatCount = 0;
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
		config.getMaster().getGlobalConfig().set("fs.settings.automergeDelayMs", 1750);
		config.getMaster().getGlobalConfig().set("fs.settings.maxAutomergeDelayMs", 5000);
		
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
	
	public ZKFSManager initCharlie() throws IOException {
		ZKFSManager manager = initPeer("charlie");
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
					log("Path mismatch: " + exc.path + " " + exc.reason);
					matches.setFalse();
				}
			});
			return matches.booleanValue();
		} catch(PathMismatchException exc) {
			log("Path mismatch: " + exc.path + " " + exc.reason);
			return false;
		}
	}
	
	public boolean peersMatch() throws IOException {
		ZKFSManager[] managers = { bob, charlie };
		for(ZKFSManager manager : managers) {
			if(manager == null) continue;
			if(!alice.getFs().getBaseRevision().equals(manager.getFs().getBaseRevision())) {
				log("Mismatched revision: "
					+ Util.formatRevisionTag(alice.getFs().getBaseRevision())
					+ " vs "
					+ Util.formatRevisionTag(bob.getFs().getBaseRevision()));
				return false;		
			}
			
			if(!fsMatch(alice.getMirror().getTarget(), manager.getMirror().getTarget())) return false;
			if(!fsMatch(manager.getMirror().getTarget(), alice.getMirror().getTarget())) return false;
		}
		
		return true;
	}
	
	public void assertPeersMatch() {
		long startTime = System.currentTimeMillis();
		assertTrue(Util.waitUntil(20000, ()->{
			try {
				return peersMatch();
			} catch(ENOENTException exc) {
				return false;
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
				return false;
			}
		}));
		log("Match found in " + (System.currentTimeMillis() - startTime));
	}

	/** This is an equivalent to the "two-peer one-way" live test, in which we have
	 * a single writer (Alice) and a single reader (Bob). They discover one another
	 * via the DHT (using a simulated central DHT node that does not participate in the
	 * archive), and synchronize an ongoing set of simple filesystem revisions discovered
	 * via Alice's FSMirror. Bob also uses FSMirror to synchronize data to disk.
	 * @throws IOException 
	 */
	@Test
	public void indefiniteTestSimpleTwoPeerEquivalent() throws IOException {
		FSTestWriter writer = new FSTestWriter(alice.getMirror().getTarget(), 2);
		long writeIntervalMs = 1500;
		
		assertTrue(Util.waitUntil(1000, ()->numConnections(alice) > 0));
		assertTrue(Util.waitUntil(1000, ()->numConnections(bob) > 0));
		
		log("Test ready.");
		long startTime = System.currentTimeMillis();
		long operations = 0;
		
		while(true) {
			if(operations++ % 10 == 0) {
				long elapsed = (System.currentTimeMillis() - startTime)/1000;
				log("Time: " + new java.util.Date());
				log("Alice revision: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
				log("  Bob revision: " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
				log(operations + " operations complete, running " + elapsed + "s");
				try {
					assertPeersMatch();
				} catch(Throwable exc) {
					log("Peers do not match");
					exc.printStackTrace();
					throw exc;
				}
				log("Peers match\n");
			}
			
			writer.act();
			Util.sleep(writeIntervalMs);
		}
	}
	
	/** Same as above, except now there are TWO readers, Bob and Charlie.
	 * This adds the complexity of Bob and Charlie each having to upload data, and
	 * receive data from multiple sources.
	 */
	@Test
	public void indefiniteTestSimpleThreePeerEquivalent() throws IOException {
		charlie = initCharlie();
		
		FSTestWriter writer = new FSTestWriter(alice.getMirror().getTarget(), 2);
		long writeIntervalMs = 1500;
		
		assertTrue(Util.waitUntil(1000, ()->numConnections(alice) >= 2));
		assertTrue(Util.waitUntil(1000, ()->numConnections(bob) >= 2));
		assertTrue(Util.waitUntil(1000, ()->numConnections(charlie) >= 2));
		
		log("3-peer test ready.");
		long startTime = System.currentTimeMillis();
		long operations = 0;
		
		while(true) {
			if(operations++ % 10 == 0) {
				long elapsed = (System.currentTimeMillis() - startTime)/1000;
				log("            Time: " + new java.util.Date());
				log("  Alice revision: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
				log("    Bob revision: " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
				log("Charlie revision: " + Util.formatRevisionTag(charlie.getFs().getBaseRevision()));
				log("\n");
				log("        Alice TX: " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getLifetimeBytes()/1024 + " KiB, " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getBytesPerSecond()/1024 + " KiB/s");
				log("          Bob TX: " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getLifetimeBytes()/1024 + " KiB, " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getBytesPerSecond()/1024 + " KiB/s");
				log("      Charlie TX: " + charlie.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getLifetimeBytes()/1024 + " KiB, " + charlie.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getBytesPerSecond()/1024 + " KiB/s");
				log("\n");
				log("        Alice RX: " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getLifetimeBytes()/1024 + " KiB, " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getBytesPerSecond()/1024 + " KiB/s");
				log("          Bob RX: " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getLifetimeBytes()/1024 + " KiB, " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getBytesPerSecond()/1024 + " KiB/s");
				log("      Charlie RX: " + charlie.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getLifetimeBytes()/1024 + " KiB, " + charlie.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getBytesPerSecond()/1024 + " KiB/s");
				log(operations + " operations complete, running " + elapsed + "s");
				try {
					assertPeersMatch();
				} catch(Throwable exc) {
					log("Peers do not match");
					exc.printStackTrace();
					throw exc;
				}
				log("Peers match\n");
			}
			
			writer.act();
			Util.sleep(writeIntervalMs);
		}
	}

	/* In this test, both Alice and Bob write continuous changes. This means data must flow in both
	 * directions, and merges must also take place.
	 */
	@Test
	public void indefiniteTestComplexTwoPeerEquivalent() throws IOException {
		FSTestWriter aliceWriter = new FSTestWriter(alice.getMirror().getTarget(), 2);
		FSTestWriter bobWriter = new FSTestWriter(bob.getMirror().getTarget(), 3);
		long writeIntervalMs = 1500;
		
		assertTrue(Util.waitUntil(1000, ()->numConnections(alice) > 0));
		assertTrue(Util.waitUntil(1000, ()->numConnections(bob) > 0));
		
		log("Test ready.");
		long startTime = System.currentTimeMillis();
		long operations = 0;
		
		while(true) {
			if(operations++ % 10 == 0) {
				long elapsed = (System.currentTimeMillis() - startTime)/1000;
				log("Time: " + new java.util.Date());
				log("Alice revision: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
				log("  Bob revision: " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
				log("\n");
				log("        Alice TX: " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getLifetimeBytes()/1024 + " KiB, " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getBytesPerSecond()/1024 + " KiB/s");
				log("          Bob TX: " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getLifetimeBytes()/1024 + " KiB, " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorTx().getBytesPerSecond()/1024 + " KiB/s");
				log("\n");
				log("        Alice RX: " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getLifetimeBytes()/1024 + " KiB, " + alice.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getBytesPerSecond()/1024 + " KiB/s");
				log("          Bob RX: " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getLifetimeBytes()/1024 + " KiB, " + bob.getFs().getArchive().getConfig().getSwarm().getBandwidthMonitorRx().getBytesPerSecond()/1024 + " KiB/s");
				log(operations + " operations complete, running " + elapsed + "s");
				try {
					assertPeersMatch();
				} catch(Throwable exc) {
					log("Peers do not match");
					System.out.println("Alice " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
					alice.getFs().getArchive().getConfig().getRevisionList().dump();
					alice.getFs().getArchive().getConfig().getRevisionList().executeAutomerge();
					System.out.println(alice.getFs().getArchive().getConfig().getRevisionList().recursiveDumpStr());
					System.out.println("Bob " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
					bob.getFs().getArchive().getConfig().getRevisionList().dump();
					bob.getFs().getArchive().getConfig().getRevisionList().executeAutomerge();
					System.out.println(bob.getFs().getArchive().getConfig().getRevisionList().recursiveDumpStr());
					exc.printStackTrace();
					throw exc;
				}
				log("Peers match: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
				log("\n");
			}
			
			aliceWriter.act();
			bobWriter.act();
			Util.sleep(writeIntervalMs);
		}
	}
}
