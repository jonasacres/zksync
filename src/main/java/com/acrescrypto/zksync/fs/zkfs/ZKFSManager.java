package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.RevisionList.RevisionMonitor;
import com.acrescrypto.zksync.fs.zkfs.ZKFS.ZKFSChangeMonitor;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class ZKFSManager implements AutoCloseable {
	protected int autocommitIntervalMs;
	protected int maxAutocommitIntervalMs = -1;
	protected boolean autocommit;
	protected boolean autofollow;
	protected boolean automirror;
	protected boolean closed;
	protected String automirrorPath, localDescription;
	
	protected ZKFS fs;
	protected ZKArchiveConfig config;
	protected SnoozeThread autocommitTimer;
	protected FSMirror mirror;
	
	protected Logger logger = LoggerFactory.getLogger(ZKFSManager.class);
	protected RevisionMonitor revMonitor;
	protected ZKFSChangeMonitor fsMonitor;
	
	protected boolean autosave;
	
	public ZKFSManager(ZKArchiveConfig config) throws IOException {
		this.localDescription = "";
		
		try {
			read(config);
		} catch(ENOENTException exc) {
			if(!config.getAccessor().isSeedOnly()) {
				if(this.fs != null) {
					this.fs.close();
				}
				
				this.fs = config.getArchive().openLatest();
			}
		}
		
		autosave = true;
		setupMonitors();
	}
	
	public ZKFSManager(ZKArchiveConfig config, byte[] serialized) throws IOException {
		deserialize(config, serialized);
		autosave = true;		
		setupMonitors();
	}
	
	public ZKFSManager(ZKFS fs) {
		this.fs = fs;
		this.localDescription = "";
		this.config = fs.getArchive().getConfig();
		setupMonitors();
	}
	
	public ZKFSManager(ZKFS fs, ZKFSManager manager) throws IOException {
		this(fs);
		
		setAutocommit          (manager.autocommit);
		setAutofollow          (manager.autofollow);
		setAutocommitIntervalMs(manager.autocommitIntervalMs);
		setAutomirrorPath      (manager.automirrorPath);
		setAutomirror          (manager.automirror);
		setLocalDescription    (manager.localDescription);
	}
	
	protected void setupMonitors() {
		this.fsMonitor  = (fs, path, stat)->notifyZKFSPathChange(path, stat);
		this.revMonitor = (revTag)->notifyNewRevtag(revTag);
		
		if(fs != null) {
			fs.addMonitor(fsMonitor);
			config.getRevisionList().addMonitor(revMonitor);
		}
	}

	public void close() throws IOException {
		closed = true;
		autosave = false;
		
		if(fs != null) {
			fs.removeMonitor(fsMonitor);
			config.getRevisionList().removeMonitor(revMonitor);
			if(!fs.isClosed()) fs.close();
		}
		
		if(mirror != null) {
			mirror.stopWatch();
		}
		
		this.setAutocommit(false);
	}
	
	public void notifyZKFSPathChange(String path, Stat stat) {
		if(autocommitTimer != null && !autocommitTimer.isExpired()) {
			logger.info("ZKFS {} {}: ZKFSManager snoozing autocommit timer, interval={}ms (id={})",
					Util.formatArchiveId(config.archiveId),
					Util.formatRevisionTag(fs.baseRevision),
					autocommitIntervalMs,
					System.identityHashCode(this)
					);
			autocommitTimer.snooze();
		}

		if(mirror == null) return;
		try {
			mirror.observedArchivePathChange(path, stat);
		} catch (IOException exc) {
			logger.error("Caught exception attempting to sync ZKFS to target for updated path {}", path, exc);
		}
	}
	
	public synchronized void notifyNewRevtag(RevisionTag revtag) {
		RevisionTag latest = config.revisionList.latest();
		boolean isDescendent = false;
		
		try {
			isDescendent = config.revisionTree.descendentOf(latest, fs.baseRevision);
			if(!isDescendent) {
				Collection<RevisionTag> baseParents = config.revisionTree.parentsForTag(fs.baseRevision);
				if(baseParents.size() > 1) {
					if(fs.baseRevision.getHeight() <= revtag.getHeight()) {
						/* If the incoming tag is at a greater height, it could still be a merge containing all
						 * of our parent merges, plus a node with a greater height. */
						boolean couldBeDescendent = true;
						for(RevisionTag parent : baseParents) {
							if(!fs.archive.config.revisionTree.descendentOf(revtag, parent)) {
								couldBeDescendent = false;
								break;
							}
						}
						
						isDescendent = couldBeDescendent;
					}
				}
			}
		} catch(IOException exc) {}
		
		if(autofollow
			&& !fs.isDirty()
			&& !fs.baseRevision.equals(latest)
			&& isDescendent) {
				try {
					logger.info("ZKFS {} {}: Automatically rebasing to new revtag {}",
							Util.formatArchiveId(fs.archive.config.archiveId),
							Util.formatRevisionTag(fs.baseRevision),
							Util.formatRevisionTag(latest));
					fs.rebase(latest);
					if(mirror != null) {
						mirror.syncArchiveToTarget();
					}
				} catch (IOException exc) {
					logger.error("ZKFS {} {}: Unable to rebase to revtag {}",
							Util.formatArchiveId(fs.archive.config.archiveId),
							Util.formatRevisionTag(fs.baseRevision),
							Util.formatArchiveId(fs.archive.config.archiveId),
							exc);
				}
		}
		
		try {
		    // we may have changed the active revision; go ahead and write the ZKFSManager to be safe
		    this.write();
		} catch(IOException exc) {
		    logger.error("ZKFS {} {}: Caught exception writing ZKFSManager",
                    Util.formatArchiveId(fs.archive.config.archiveId),
                    Util.formatRevisionTag(fs.baseRevision),
		            exc);
		}
	}
	
	public ZKFS getFs() {
		return fs;
	}

	public int getAutocommitIntervalMs() {
		return autocommitIntervalMs;
	}
	
	public int getMaxAutocommitIntervalMs() {
		return maxAutocommitIntervalMs;
	}
	
	public String getLocalDescription() {
		return localDescription;
	}

	public void setAutocommitIntervalMs(int autocommitIntervalMs) {
		if(this.autocommitIntervalMs == autocommitIntervalMs) return;
		this.autocommitIntervalMs = autocommitIntervalMs;
		
	    logger.debug("ZKFS {} -: ZKFSManager set autocommitIntervalMs={}",
                Util.formatArchiveId(config.getArchiveId()),
                autocommitIntervalMs);
	      
		setupAutocommitTimer();
		autosaveIfDesired();
	}
	
	public void setMaxAutocommitIntervalMs(int maxAutocommitIntervalMs) {
		if(this.maxAutocommitIntervalMs == maxAutocommitIntervalMs) return;
		this.maxAutocommitIntervalMs = maxAutocommitIntervalMs;
		
		logger.debug("ZKFS {} -: ZKFSManager set maxAutocommitIntervalMs={}",
		        Util.formatArchiveId(config.getArchiveId()),
		        maxAutocommitIntervalMs);
		
		setupAutocommitTimer();
		autosaveIfDesired();
	}

	public boolean isAutocommiting() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		if(this.autocommit == autocommit) return;
		
        this.autocommit = autocommit;
        logger.debug("ZKFS {} -: ZKFSManager set autocommit={}",
                Util.formatArchiveId(config.getArchiveId()),
                autocommit);
        
		setupAutocommitTimer();
		autosaveIfDesired();
	}
	
	public void setLocalDescription(String localDescription) {
		if(this.localDescription == null && localDescription == null) return;
		if(this.localDescription != null && this.localDescription.equals(localDescription)) return;

	    this.localDescription = localDescription;
        
	    logger.debug("ZKFS {} -: ZKFSManager set localDescription={}",
                Util.formatArchiveId(config.getArchiveId()),
                localDescription);

        autosaveIfDesired();
	}

	public boolean isAutofollowing() {
		return autofollow;
	}

	public void setAutofollow(boolean autofollow) {
		if(this.autofollow == autofollow) return;
		this.autofollow = autofollow;
		
		logger.debug("ZKFS {} -: ZKFSManager set autofollow={}",
		        Util.formatArchiveId(config.getArchiveId()),
		        autofollow);

		autosaveIfDesired();
	}
	
	protected void setupAutocommitTimer() {
		if(closed || !autocommit || autocommitIntervalMs <= 0) {
			if(autocommitTimer != null) {
				autocommitTimer.cancel();
				autocommitTimer = null;
			}
			
			if(fs != null) {
				logger.info("ZKFS {} {}: ZKFSManager disabling autocommit (id={})",
						fs == null ? "-" : Util.formatArchiveId(fs.archive.config.archiveId),
						fs == null ? "-" : Util.formatRevisionTag(fs.baseRevision),
						System.identityHashCode(this)
						);
			}
			
			return;
		}
		
		if(autocommitTimer != null) {
			autocommitTimer.cancel();
		}
		
		logger.debug("ZKFS {} {}: ZKFSManager starting autocommit, interval={}ms (id={})",
				Util.formatArchiveId(fs.archive.config.archiveId),
				Util.formatRevisionTag(fs.baseRevision),
				autocommitIntervalMs,
				System.identityHashCode(this)
				);
		autocommitTimer = new SnoozeThread(autocommitIntervalMs, maxAutocommitIntervalMs, false, ()->{
			try {
				if(fs.isDirty()) {
					logger.info("ZKFS {} {}: ZKFSManager triggering autocommit (id={})",
							Util.formatArchiveId(fs.archive.config.archiveId),
							Util.formatRevisionTag(fs.baseRevision),
							System.identityHashCode(this)
							);
					fs.commit();
				}
				
				setupAutocommitTimer();
			} catch (IOException exc) {
				exc.printStackTrace();
				logger.error("ZKFS {} {}: IOException performing autocommit",
						Util.formatArchiveId(fs.archive.config.archiveId),
						Util.formatRevisionTag(fs.baseRevision),
						exc);
			}
		});
	}

	public boolean isAutomirroring() {
		return automirror;
	}

	public void setAutomirror(boolean automirror) throws IOException {
		if(automirror == this.automirror) return;
		if(automirrorPath == null) {
			throw new EINVALException("No automirror path set");
		}
		
		if(automirror && this.automirrorPath != null) {
			Stat stat = mirror
					.getTarget()
					.stat("/");
			if(!stat.isDirectory()) {
				throw new EISNOTDIRException(this.automirrorPath);
			}
			mirror.startWatch();
		} else {
			mirror.stopWatch();
		}

		this.automirror = automirror;
		
		logger.debug("ZKFS {} -: ZKFSManager set automirror={} (path: {})",
                Util.formatArchiveId(config.getArchiveId()),
                automirror,
                automirrorPath);
		
		autosaveIfDesired();
	}

	public String getAutomirrorPath() {
		return automirrorPath;
	}

	public void setAutomirrorPath(String automirrorPath) throws IOException {
		this.automirrorPath = automirrorPath;
		if(mirror != null) {
			mirror.stopWatch();
		}
		
		if(automirrorPath != null) {
			LocalFS localFs = new LocalFS(automirrorPath);
			
			if(mirror == null) {
				mirror = new FSMirror(fs, localFs);
			} else {
				mirror.setTargetFs(localFs);
				mirror.setZkfs(fs);
			}
			
			if(automirror) {
				mirror.startWatch();
			}
		} else {
			mirror = null;
			automirror = false;
		}

		logger.debug("ZKFS {} -: ZKFSManager set automirrorPath={} (enabled: {})",
                Util.formatArchiveId(config.getArchiveId()),
                automirrorPath,
                automirror);
		
		autosaveIfDesired();
	}
	
	protected String path() {
		return "manager";
	}
	
	protected Key storageKey(ZKArchiveConfig config) {
		return config.deriveKey(ArchiveAccessor.KEY_ROOT_LOCAL,
				"easysafe-local-storage-key");
	}
	
	public void write() throws IOException {
		if(config == null) return;
		logger.debug("ZKFS {} -: ZKFSManager writing settings",
                Util.formatArchiveId(config.getArchiveId()));
		
		MutableSecureFile
		  .atPath(config.getLocalStorage(),
				path(),
				storageKey(config))
		  .write(serialize(), 65536);
	}
	
	protected void autosaveIfDesired() {
		if(!autosave) return;
		try {
			write();
		} catch(IOException exc) {
			logger.error("ZKFS {} {}: Unable to write ZKFSManager file",
					Util.formatArchiveId(fs.archive.config.archiveId),
					Util.formatRevisionTag(fs.baseRevision),
					exc);
		}
	}
	
	protected void read(ZKArchiveConfig config) throws IOException {
        this.config = config;
		byte[] contents = MutableSecureFile
		  .atPath(config.getLocalStorage(),
				path(),
				storageKey(config))
		  .read();
		deserialize(config, contents);
	}
	
	protected byte[] serialize() {
		JsonObjectBuilder builder = Json
				.createObjectBuilder();

		builder.add("autocommit",           autocommit);
		builder.add("autofollow",           autofollow);
		builder.add("automirror",           automirror);
		builder.add("automerge",            isAutomerging());
		builder.add("autocommitIntervalMs", autocommitIntervalMs);
		
		builder.add("advertising",          config.isAdvertising());
		builder.add("requestingAll",        config.getSwarm().isRequestingAll());
		builder.add("peerLimit",            config.getSwarm().getMaxSocketCount());
		
		if(localDescription != null) {
			builder.add("localDescription", localDescription);
		}
		
		if(automirrorPath != null) {
			builder.add("automirrorPath",   automirrorPath);
		}
		
		if(fs != null) {
		    builder.add("revtag",           Util.encode64(fs.getBaseRevision().serialize()));
		}
		
		return builder.build().toString().getBytes();
	}
	
	protected void deserialize(ZKArchiveConfig config, byte[] serialized) throws IOException {
	    this.config       = config;
        try {
    		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
    		JsonObject json   = reader.readObject();
    		
    		if(!config.getAccessor().isSeedOnly()) {
        		String tagRaw = json.getString("revtag", null);
        		
        		if(tagRaw != null) {
        			RevisionTag revTag = new RevisionTag(config, Util.decode64(tagRaw), true);
        			fs = revTag.getFS();
        		} else {
        			fs = config.getArchive().openLatest();
        		}
    		}
    		
    		setAutocommit          (json.getBoolean("autocommit"));
    		setAutofollow          (json.getBoolean("autofollow"));
    		setAutomerge           (json.getBoolean("automerge"));
    		setAutocommitIntervalMs(json.getInt    ("autocommitIntervalMs"));
    		setLocalDescription    (json.getString ("localDescription", ""));
    		
    		if(json.getBoolean("advertising", false)) {
    			config.advertise();
    		} else {
    			config.stopAdvertising();
    		}
    		
    		if(json.getBoolean("requestingAll", false)) {
    			config.swarm.requestAll();
    		} else {
    			config.swarm.stopRequestingAll();
    		}
    		
    		if(json.containsKey("peerLimit")) {
    			config.swarm.setMaxSocketCount(json.getInt("peerLimit"));
    		}
    		
    		if(json.containsKey("automirrorPath")) {
    			setAutomirrorPath(json.getString("automirrorPath"));
    		} else {
    			setAutomirrorPath(null);
    		}
    		setAutomirror(json.getBoolean("automirror"));
        } catch(Exception exc) {
            logger.error("ZKFS {} -: Caught exception parsing ZKFSManager configuration",
                    Util.formatArchiveId(config.getArchiveId()),
                    exc);
        }
	}

	public void setFs(ZKFS fs) throws IOException {
		if(fs == this.fs) return;
		
		this.fs.close();
		this.fs = fs.retain();
		setupMonitors();
		setAutomirrorPath(this.automirrorPath); // reinit mirror
		if(mirror != null) {
			try {
				mirror.syncArchiveToTarget();
			} catch(IOException exc) {
				logger.info("ZKFS {} {}: Failed to mirror changes",
						Util.formatArchiveId(fs.archive.config.archiveId),
						Util.formatRevisionTag(fs.baseRevision),
						exc);
			}
		}
		// setting the path already called autosaveIfDesired()
	}

	public void purge() throws IOException {
		close();
		if(fs == null) return;
		if(config.getLocalStorage().exists(path())) {
			config.getLocalStorage().unlink(path());
		}
	}

	public boolean isAutomerging() {
		return config.getRevisionList().getAutomerge();
	}
	
	public void setAutomerge(boolean automerging) {
		config.getRevisionList().setAutomerge(automerging);
	}
	
	public FSMirror getMirror() {
		return mirror;
	}
}
