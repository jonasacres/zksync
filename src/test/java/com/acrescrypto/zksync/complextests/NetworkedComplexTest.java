package com.acrescrypto.zksync.complextests;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
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
		FS.fileHandleTelemetryEnabled = false;
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
		if(alice != null) {
			alice.close();
		}
		
		if(bob != null) {
			bob.close();
		}
		
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
	
	public boolean randomChance(double p) {
		return Math.random() <= p;
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
		master.setName(peerName);
		master.getGlobalConfig().set("net.swarm.enabled", true);
		master.activateDHT("127.0.0.1", 0, dhtRoot.getDHTClient().getLocalPeer());
		ZKArchive archive = master.createDefaultArchive("NetworkedComplexTest".getBytes());
		archive.getConfig().advertise();
		return archive.getConfig();
	}
	
	public ZKFSManager initPeer(String peerName) throws IOException {
		try(LocalFS fs = new LocalFS("/")) {
			fs.mkdirp(automirrorPath(peerName));
		}

		ZKFSManager manager = initPeerZKFSOnly(peerName);
		manager.setAutomirrorPath(automirrorPath(peerName));
		manager.setAutomirror(true);
		
		return manager;
	}
	
	public ZKFSManager initPeerZKFSOnly(String peerName) throws IOException {
		try(LocalFS fs = new LocalFS("/")) {
			fs.mkdirp(encryptedDataPath(peerName));
		}

		ZKArchiveConfig config = initConfig(peerName);
		ZKFSManager manager = new ZKFSManager(config);
		
		manager.setAutofollow(true);
		manager.setAutocommit(true);
		manager.setAutocommitIntervalMs(1000);
		manager.setMaxAutocommitIntervalMs(3000);
		manager.setAutomirror(false);
		manager.setAutomerge(true);
		config.getMaster().getGlobalConfig().set("fs.settings.automergeDelayMs", 1750);
		config.getMaster().getGlobalConfig().set("fs.settings.maxAutomergeDelayMs", 5000);
		config.getMaster().getGlobalConfig().set("fs.settings.mergeRevisionAcquisitionMaxWaitMs", 5000);
		config.getMaster().getGlobalConfig().set("fs.settings.maxAutomergeAcquireWaitTimeMs", 5000);
		config.getMaster().getGlobalConfig().set("fs.settings.readOnlyFilesystemCacheSize", 512);
		
		return manager;
	}
	
	public void closePeer(ZKFSManager peer) throws IOException {
		peer.close();
		peer.getFs().getArchive().close();
		// peer.getFs().getArchive().getMaster().close();
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
	
	public boolean fsMatch(FS ref, FS comp, String refName, String compName, boolean dump) throws IOException {
		MutableBoolean matches = new MutableBoolean();
		matches.setTrue();
		
		try(Directory dir = ref.opendir("/")) {
			dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS, (path, stat, isBroken, parent)->{
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
					Stat compStat = null, refStat = null;
					String out = "";
					out += "Path mismatch: " + exc.path + " " + exc.reason + "\n";

					try {
						compStat = comp.lstat(exc.path);
						out += String.format("  Comparison copy (%12s): %d bytes, inode %d\n",
								compName,
								compStat.getSize(),
								compStat.getInodeId());
					} catch(ENOENTException exc2) {
						out += "  Comparison copy: ENOENT\n";
					}
					
					try {
						refStat = ref.lstat(exc.path);
						out += String.format("   Reference copy (%12s): %d bytes, inode %d\n",
								refName,
								refStat.getSize(),
								refStat.getInodeId());
					} catch(ENOENTException exc2) {
						out += "   Reference copy: ENOENT\n";
					}
					
					if(dump) log(out);
					matches.setFalse();
				}
			});
			return matches.booleanValue();
		} catch(PathMismatchException exc) {
			if(dump) log("Path mismatch: " + exc.path + " " + exc.reason);
			return false;
		}
	}
	
	public boolean peersMatch(boolean dump) throws IOException {
		ZKFSManager[] managers = { bob, charlie };
		for(ZKFSManager manager : managers) {
			if(manager == null) continue;
			if(!alice.getFs().getBaseRevision().equals(manager.getFs().getBaseRevision())) {
				if(dump) {
					log("Mismatched revision: "
						+ Util.formatRevisionTag(alice.getFs().getBaseRevision())
						+ " vs "
						+ Util.formatRevisionTag(bob.getFs().getBaseRevision()));
				}
				return false;
			}
			
			if(!fsMatch(alice.getMirror().getTarget(), manager.getMirror().getTarget(), "Alice", "Bob", dump)) return false;
			if(!fsMatch(manager.getMirror().getTarget(), alice.getMirror().getTarget(), "Bob", "Alice", dump)) return false;
		}
		
		return true;
	}
	
	public boolean multiPeersMatch(ZKFSManager[] managers, boolean dump) throws IOException {
		for(ZKFSManager manager : managers) {
			if(manager == managers[0]) continue;
			if(!managers[0].getFs().getBaseRevision().equals(manager.getFs().getBaseRevision())) {
				if(dump) {
					log("Mismatched revision: "
						+ Util.formatRevisionTag(managers[0].getFs().getBaseRevision())
						+ " vs "
						+ Util.formatRevisionTag(manager.getFs().getBaseRevision()));
				}
				return false;
			}
			
			String refName = managers[0].getFs().getArchive().getMaster().getName();
			String compName = manager.getFs().getArchive().getMaster().getName();
			
			if(!fsMatch(managers[0].getMirror().getTarget(), manager.getMirror().getTarget(), refName, compName, dump)) return false;
			if(!fsMatch(manager.getMirror().getTarget(), managers[0].getMirror().getTarget(), compName, refName, dump)) return false;
		}
		
		return true;
	}
	
	public String multiPeersReport(ZKFSManager[] peers) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < peers.length; i++) {
			ZKArchive archive = peers[i].getFs().getArchive();
			sb.append(String.format("\t%20s: Revision %16s | TX %8d KiB, %4d KiB/s | RX %8d KiB, %4d KiB/s\n",
				archive.getMaster().getName(),
				Util.formatRevisionTag(peers[i].getFs().getBaseRevision()),
				archive.getConfig().getSwarm().getBandwidthMonitorTx().getLifetimeBytes()/1024,
				archive.getConfig().getSwarm().getBandwidthMonitorTx().getBytesPerSecond()/1024,
				archive.getConfig().getSwarm().getBandwidthMonitorRx().getLifetimeBytes()/1024,
				archive.getConfig().getSwarm().getBandwidthMonitorRx().getBytesPerSecond()/1024));
		}
		return sb.toString();
	}
	
	public void assertPeersMatch() {
		log("Testing match...");
		long startTime = System.currentTimeMillis();
		if(!Util.waitUntil(10000, ()->{
			try {
				return peersMatch(false);
			} catch(ENOENTException exc) {
				return false;
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
				return false;
			}
		})) {
			log("!!! PEERS FAILED TO MATCH\n\n\n\n\n");
			try {
				peersMatch(true);
			} catch(ENOENTException exc) {
				log("ENOENT: " + exc.getMessage());
			} catch(IOException exc) {
				exc.printStackTrace();
			}
			
			fail("Peers did not match");
		}
		log("Match found in " + (System.currentTimeMillis() - startTime));
	}
	
	public void assertMultiPeersMatch(ZKFSManager[] peers, int interval) {
		log("Testing match in " + peers.length + " peers...");
		long startTime = System.currentTimeMillis();
		if(!Util.waitUntil(interval, ()->{
			try {
				return multiPeersMatch(peers, false);
			} catch(ENOENTException|FileSystemException|FileNotFoundException exc) {
				return false;
			} catch (IOException exc) {
				exc.printStackTrace();
				fail();
				return false;
			}
		})) {
			log("!!! PEERS FAILED TO MATCH\n\n\n\n\n");
			try {
				multiPeersMatch(peers, true);
				for(ZKFSManager peer : peers) {
					peer.getFs().getArchive().getConfig().getRevisionList().dumpDot();
				}
			} catch(ENOENTException exc) {
				log("ENOENT: " + exc.getMessage());
			} catch(IOException exc) {
				exc.printStackTrace();
			}
			
			fail("Peers did not match");
		}
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
		FSTestWriter writer = new FSTestWriter(alice.getMirror().getTarget(), 2, "alice");
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
		
		FSTestWriter writer = new FSTestWriter(alice.getMirror().getTarget(), 2, "alice");
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

	/* In this test, both Alice and Bob take turns writing changes, then we wait to ensure sync happens.
	 * This ensures data flows correctly in both directions, but eliminates the need for merges.
	 */
	@Test
	public void indefiniteTestComplexTwoPeerPingPongEquivalent() throws IOException {
		FSTestWriter aliceWriter = new FSTestWriter(alice.getMirror().getTarget(), 2, "alice");
		FSTestWriter bobWriter = new FSTestWriter(bob.getMirror().getTarget(), 3, "bob");
		long writeIntervalMs = 1500;
		
		assertTrue(Util.waitUntil(1000, ()->numConnections(alice) > 0));
		assertTrue(Util.waitUntil(1000, ()->numConnections(bob) > 0));
		
		log("Test ready.");
		long startTime = System.currentTimeMillis();
		long operations = 0;
		boolean alicesTurn = true;
		
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
					System.out.println("Before manual merge");
					System.out.println("Alice " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
					alice.getFs().getArchive().getConfig().getRevisionList().dump();

					System.out.println("Bob " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
					bob.getFs().getArchive().getConfig().getRevisionList().dump();
					
					System.out.println("Automerging");
					alice.getFs().getArchive().getConfig().getRevisionList().executeAutomerge();
					System.out.println(alice.getFs().getArchive().getConfig().getRevisionList().recursiveDumpStr());
					bob.getFs().getArchive().getConfig().getRevisionList().executeAutomerge();
					System.out.println(bob.getFs().getArchive().getConfig().getRevisionList().recursiveDumpStr());
					System.out.println("Alice " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
					alice.getFs().getArchive().getConfig().getRevisionList().dump();
					System.out.println("Bob " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
					bob.getFs().getArchive().getConfig().getRevisionList().dump();
					exc.printStackTrace();
					throw exc;
				}
				log("Peers match: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
				log("\n");
				alicesTurn = !alicesTurn;
			}
			
			if(alicesTurn) {
				aliceWriter.act();
			} else {
				bobWriter.act();
			}
			
			Util.sleep(writeIntervalMs);
		}
	}

	/* In this test, both Alice and Bob write continuous changes. This means data must flow in both
	 * directions, and merges must also take place.
	 */
	@Test
	public void indefiniteTestComplexTwoPeerEquivalent() throws IOException {
		FSTestWriter aliceWriter = new FSTestWriter(alice.getMirror().getTarget(), 2, "alice");
		FSTestWriter bobWriter = new FSTestWriter(bob.getMirror().getTarget(), 3, "bob");
		long writeIntervalMs = 100;
		
		assertTrue(Util.waitUntil(1000, ()->numConnections(alice) > 0));
		assertTrue(Util.waitUntil(1000, ()->numConnections(bob) > 0));
		
		log("Test ready.");
		long startTime = System.currentTimeMillis();
		long operations = 0;
		long lastCheck = System.currentTimeMillis();
		
		while(true) {
			if(System.currentTimeMillis() - lastCheck > 30000) {
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
					System.out.println("Before manual merge");
					System.out.println("Alice " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
					alice.getFs().getArchive().getConfig().getRevisionList().dump();

					System.out.println("Bob " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
					bob.getFs().getArchive().getConfig().getRevisionList().dump();
					
					System.out.println("Automerging");
					alice.getFs().getArchive().getConfig().getRevisionList().executeAutomerge();
					System.out.println(alice.getFs().getArchive().getConfig().getRevisionList().recursiveDumpStr());
					bob.getFs().getArchive().getConfig().getRevisionList().executeAutomerge();
					System.out.println(bob.getFs().getArchive().getConfig().getRevisionList().recursiveDumpStr());
					System.out.println("Alice " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
					alice.getFs().getArchive().getConfig().getRevisionList().dump();
					System.out.println("Bob " + Util.formatRevisionTag(bob.getFs().getBaseRevision()));
					bob.getFs().getArchive().getConfig().getRevisionList().dump();
					
					log("Final peer match: " + peersMatch(true));
					
					exc.printStackTrace();
					throw exc;
				}
				log("Peers match: " + Util.formatRevisionTag(alice.getFs().getBaseRevision()));
				log("\n");
				lastCheck = System.currentTimeMillis();
			}
			
			aliceWriter.act();
			bobWriter.act();
			Util.sleep(writeIntervalMs);
		}
	}
	
	/* N peers simultaneously make changes. Exercises data flow in many directions with complex merges.
	 */
	@Test
	public void indefiniteTestComplexManyPeerConstantMembershipEquivalent() throws IOException {
		int numPeers = 5;
		FSTestWriter[] writers = new FSTestWriter[numPeers];
		ZKFSManager[] peers = new ZKFSManager[numPeers];
		
		// close up the default peers
		closePeer(alice);
		alice = null;
		
		closePeer(bob);
		bob = null;
		
		for(int i = 0; i < numPeers; i++) {
			String name = String.format("peer-%02d", i);
			peers[i] = initPeer(name);
			writers[i] = new FSTestWriter(peers[i].getMirror().getTarget(), i, name);
		}
		
		long writeIntervalMs = 500;
		
		for(ZKFSManager peer : peers) {
			assertTrue(Util.waitUntil(1000, ()->numConnections(peer) > 0));
		}
		
		log("Test ready. " + numPeers + " peers.");
		long startTime = System.currentTimeMillis();
		long operations = 0;
		long lastCheck = System.currentTimeMillis();
		
		while(true) {
			if(System.currentTimeMillis() - lastCheck > 30000) {
				long elapsed = (System.currentTimeMillis() - startTime)/1000;
				log(String.format("Pre-merge report at %ds, %d operations\n%s",
						elapsed,
						operations,
						multiPeersReport(peers)));
				
				try {
					assertMultiPeersMatch(peers, 30000);
				} catch(Throwable exc) {
					log("Peers do not match");
					
					exc.printStackTrace();
					throw exc;
				}
				log("Peers match: " + Util.formatRevisionTag(peers[0].getFs().getBaseRevision()));
				log("\n");
				log(String.format("Post-merge report at %ds, %d operations\n%s\n",
						elapsed,
						operations,
						multiPeersReport(peers)));
				
				lastCheck = System.currentTimeMillis();
			}
			
			for(FSTestWriter writer : writers) {
				writer.act();
			}
			
			operations++;
			Util.sleep(writeIntervalMs);
		}
	}
	
	@Test
	public void indefiniteTestComplexZKFSOnlyManyPeerVariableMembershipEquivalent() throws IOException {
		int checkIntervalMs = 30000;
		int numPeersInit = 4;

		int peerNum = 0;
		CryptoSupport crypto = CryptoSupport.defaultCrypto();
		LinkedList<ZKFSManager> peers = new LinkedList<>();
		HashMap<ZKFSManager,FSTestWriter> writers = new HashMap<>();
		// close up the default peers
		closePeer(alice);
		alice = null;
		
		closePeer(bob);
		bob = null;
		
		for(int i = 0; i < numPeersInit; i++) {
			ZKFSManager peer = initPeerZKFSOnly("peer-" + (peerNum));
			peers.add(peer);
			writers.put(peer, new FSTestWriter(peer.getFs(),
					peerNum,
					peer.getFs().getArchive().getMaster().getName()));
			peerNum++;
		}
		
		int checkNum = 0;
		long lastCheck = System.currentTimeMillis();
		
		while(true) {
			if(randomChance(0.00) || peers.isEmpty()) {
				if(peers.isEmpty() || randomChance(0.5)) {
					// start a new peer
					String name = "peer-" + peerNum;
					Util.debugLog("Starting peer " + name);
					ZKFSManager peer = initPeerZKFSOnly(name);
					peers.add(peer);
					writers.put(peer, new FSTestWriter(peer.getFs(),
							peerNum,
							peer.getFs().getArchive().getMaster().getName()));
					peerNum++;
				} else {
					// stop a random peer
					int index = crypto.defaultPrng().getInt(peers.size());
					ZKFSManager peer = peers.remove(index);
					writers.remove(peer);
					Util.debugLog("Stopped peer " + peer.getFs().getArchive().getMaster().getName());
					closePeer(peer);
				}
			}
			
			if(!peers.isEmpty() && randomChance(0.01)) {
				int index = crypto.defaultPrng().getInt(peers.size());
				ZKFSManager peer = peers.get(index);
				writers.get(peer).act();
			}
			
			if(System.currentTimeMillis() >= lastCheck + checkIntervalMs) {
				++checkNum;
				Util.debugLog(String.format("Starting check %d",
						checkNum));
				boolean passed = Util.waitProgressively(30000, peers.size(), ()->{
					if(peers.isEmpty()) return 0;
					HashMap<RevisionTag,Integer> counts = new HashMap<>();
					
					for(ZKFSManager peer : peers) {
						RevisionTag revtag = peer.getFs().getBaseRevision();
						counts.put(revtag, counts.getOrDefault(revtag, 0) + 1);
					}
					
					int max = 0;
					for(Integer count : counts.values()) {
						if(count > max) max = count;
					}
					
					return max;
				});
				
				if(!passed) {
					Util.debugLog(String.format("Check %d failed. %d peers",
							checkNum,
							peers.size()));
					for(ZKFSManager peer : peers) {
						Util.debugLog(String.format("\tPeer %s: %s",
								peer.getFs().getArchive().getMaster().getName(),
								Util.formatRevisionTag(peer.getFs().getBaseRevision())));
						// peer.getFs().getArchive().getConfig().getRevisionList().dumpDot();
					}
					fail();
				}
				
				Util.debugLog(String.format("Check %d passed. %d peers, revtag %s",
						checkNum,
						peers.size(),
						peers.isEmpty()
								? "N/A"
								: Util.formatRevisionTag(peers.getFirst().getFs().getBaseRevision())));
				lastCheck = System.currentTimeMillis();
			}
			
			Util.sleep(1);
		}
	}
}
