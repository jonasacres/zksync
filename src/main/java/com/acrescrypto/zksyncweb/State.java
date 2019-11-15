package com.acrescrypto.zksyncweb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.InvalidRevisionTagException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;
import com.acrescrypto.zksync.fs.zkfs.PassphraseProvider;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSManager;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.utility.Util;

public class State implements AutoCloseable {
	public class TrackedFS {
		ZKFS fs;
		ZKFSManager manager;
		
		public TrackedFS(ZKFS fs) {
			this.fs = fs;
			this.manager = new ZKFSManager(fs);
		}
		
		public TrackedFS(ZKFS fs, TrackedFS existing) throws IOException {
			this.fs = fs;
			this.manager = new ZKFSManager(fs, existing.manager);
		}
	}
	
	private static State sharedState;
	protected Logger logger = LoggerFactory.getLogger(State.class);
	
	public static byte[] defaultPassphrase() {
		// TODO Someday: (redesign) allow specification of local key
		return null;
	}
	
	public static CryptoSupport sharedCrypto() throws IOException {
		return sharedState().getMaster().getCrypto();
	}
	
	public static State sharedState() throws IOException {
		if(sharedState == null) {
			sharedState = new State(defaultPassphrase(), System.getProperty("user.dir") + "/data");
		}
		
		return sharedState;
	}
	
	public static void clearState() {
		if(sharedState == null) return;
		try {
			sharedState.close();
		} catch (IOException e) {}
		sharedState = null;
	}
	
	public static State setTestState() throws IOException {
		clearState();
		return sharedState = new State(defaultPassphrase(), new RAMFS());
	}
	
	public static void resetState() throws IOException {
		FS storage = sharedState.getMaster().getStorage();
		clearState();
		sharedState = new State(defaultPassphrase(), storage);
	}
	
	class OneTimePassphraseProvider implements PassphraseProvider {
		byte[] passphrase;
		public OneTimePassphraseProvider(byte[] passphrase) {
			this.passphrase = passphrase;
		}
		
		@Override
		public byte[] requestPassphrase(String purpose) {
			return passphrase;
		}
	}
	
	private ZKMaster master;
	private Map<ZKArchiveConfig, ZKFSManager> activeFilesystems = new ConcurrentHashMap<>();
	
	public State(byte[] passphrase, String path) throws IOException {
		this.master = ZKMaster.openAtPath(new OneTimePassphraseProvider(passphrase), path);
		initManagers();
	}
	
	public State(byte[] passphrase, FS storage) throws IOException {
		this.master = ZKMaster.open(new OneTimePassphraseProvider(passphrase), storage);
		initManagers();
	}
	
	public ZKMaster getMaster() {
		return master;
	}
	
	public void close() throws IOException {
		for(ZKArchiveConfig config : activeFilesystems.keySet()) {
			activeFilesystems.get(config).close();
			config.close();
		}
		
		activeFilesystems.clear();
		
		master.close();
		master = null;
	}
	
	public Collection<ZKArchiveConfig> getOpenConfigs() {
		return new ArrayList<>(master.allConfigs());
	}
	
	public void addOpenConfig(ZKArchiveConfig config) {
		master.discoveredArchiveConfig(config);
	}
	
	public ZKArchiveConfig configForArchiveId(String archiveId) {
		// TODO API: (refactor) need an O(1) way of doing this
		
		archiveId = Util.fromWebSafeBase64(archiveId);
		for(ZKArchiveConfig config : getOpenConfigs()) {
			String b64 = Base64.getEncoder().encodeToString(config.getArchiveId());
			if(b64.startsWith(archiveId)) return config;
		}
		
		return null;
	}
	
	public ZKArchiveConfig configForArchiveId(byte[] archiveId) {
		for(ZKArchiveConfig config : getOpenConfigs()) {
			if(Arrays.equals(config.getArchiveId(), archiveId)) {
				try {
					activeFs(config);
				} catch (IOException exc) {
					logger.error("Caught exception instantiating active FS for archive {}",
							Util.bytesToHex(archiveId),
							exc);
				}
				return config;
			}
		}
		
		return null;
	}
	
	public ZKFS activeFs(ZKArchiveConfig config) throws IOException {
		if(activeFilesystems.containsKey(config)) {
			ZKFS fs = activeFilesystems.get(config).getFs();
			if(!fs.isClosed()) {
				return activeFilesystems.get(config).getFs();
			}
		}
		
		synchronized(this) {
			if(activeFilesystems.containsKey(config)) {
				ZKFS fs = activeFilesystems.get(config).getFs();
				if(!fs.isClosed()) {
					return fs;
				}
				
				activeFilesystems.remove(config);
			}
			
			if(config.getArchive() == null) return null;
			ZKFSManager manager = new ZKFSManager(config);
			activeFilesystems.put(config, manager);
			return manager.getFs();
		}
	}
	
	public synchronized void setActiveFs(ZKArchiveConfig config, ZKFS fs) throws IOException {
		ZKFSManager manager = activeManager(config);
		manager.setFs(fs);
	}
	
	public ZKFS fsForRevision(ZKArchiveConfig config, String revTag64) throws IOException {
		// TODO API: (refactor) A cache would be nice here.
		revTag64 = Util.fromWebSafeBase64(revTag64);
		byte[] rawBytes;
		try {
			rawBytes = Util.decode64(revTag64);
		} catch(IllegalArgumentException exc) {
			// if the thing can't deserialize, see if it works as a string prefix
			rawBytes = null;
		}
		
		RevisionTag revTag;
		if(rawBytes != null && rawBytes.length == RevisionTag.sizeForConfig(config)) {
			// deserialize the revtag if we got a whole revtag
			revTag = new RevisionTag(config, rawBytes, false);
		} else {
			// but if we only got a prefix (or something we can't decode), try looking for a match
			revTag = config.getRevisionTree().tagWithPrefix(revTag64);
			if(revTag == null) {
				throw new InvalidRevisionTagException(revTag64);
			}
		}
		
		return revTag.getFS();
	}
	
	public ZKFSManager activeManager(ZKArchiveConfig config) throws IOException {
		activeFs(config); // automatically instantiate if we don't have it
		return activeFilesystems.get(config);
	}

	public void removeConfig(ZKArchiveConfig config) {
		if(!config.isClosed()) {
			config.close();
		}
		
		synchronized(this) {
			ZKFSManager manager = activeFilesystems.get(config);
			
			if(manager != null) {
				activeFilesystems.remove(config);
				try {
					manager.purge();
				} catch (IOException exc) {
				}
			}
		}
		
		master.removedArchiveConfig(config);
	}
	
	protected void initManagers() throws IOException {
		for(ZKArchiveConfig config : getOpenConfigs()) {
			activeManager(config);
		}
	}
}
