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
	protected String automirrorPath;
	
	protected ZKFS fs;
	protected SnoozeThread autocommitTimer;
	protected FSMirror mirror;
	
	protected Logger logger = LoggerFactory.getLogger(ZKFSManager.class);
	protected RevisionMonitor revMonitor;
	protected ZKFSChangeMonitor fsMonitor;
	
	protected boolean autosave;
	
	public ZKFSManager(ZKArchiveConfig config) throws IOException {
		try {
			read(config);
		} catch(ENOENTException exc) {
			if(!config.getAccessor().isSeedOnly()) {
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
		setupMonitors();
	}
	
	public ZKFSManager(ZKFS fs, ZKFSManager manager) throws IOException {
		this(fs);
		
		setAutocommit(manager.autocommit);
		setAutofollow(manager.autofollow);
		setAutocommitIntervalMs(manager.autocommitIntervalMs);
		setAutomirrorPath(manager.automirrorPath);
		setAutomirror(manager.automirror);
	}
	
	protected void setupMonitors() {
		this.fsMonitor = (fs, path)->notifyZKFSPathChange(path);
		this.revMonitor = (revTag)->notifyNewRevtag(revTag);
		
		if(fs != null) {
			fs.addMonitor(fsMonitor);
			fs.getArchive().getConfig().getRevisionList().addMonitor(revMonitor);
		}
	}

	public void close() throws IOException {
		if(fs != null) {
			fs.removeMonitor(fsMonitor);
			fs.getArchive().getConfig().getRevisionList().removeMonitor(revMonitor);
			if(!fs.isClosed()) fs.close();
		}
		
		if(mirror != null) {
			mirror.stopWatch();
		}
	}
	
	public synchronized void notifyZKFSPathChange(String path) {
		if(autocommitTimer != null && !autocommitTimer.isExpired()) {
			logger.info("ZKFS {} {}: ZKFSManager snoozing autocommit timer, interval={}ms (id={})",
					Util.formatArchiveId(fs.archive.config.archiveId),
					Util.formatRevisionTag(fs.baseRevision),
					autocommitIntervalMs,
					System.identityHashCode(this)
					);
			autocommitTimer.snooze();
		}

		if(mirror == null) return;
		try {
			mirror.observedArchivePathChange(path);
		} catch (IOException exc) {
			logger.error("Caught exception attempting to sync ZKFS to target for updated path {}", path, exc);
		}
	}
	
	public synchronized void notifyNewRevtag(RevisionTag revtag) {
		RevisionTag latest = fs.archive.config.revisionList.latest();
		boolean isDescendent = false;
		
		try {
			isDescendent = fs.archive.config.revisionTree.descendentOf(latest, fs.baseRevision);
			if(!isDescendent) {
				Collection<RevisionTag> baseParents = fs.archive.config.revisionTree.parentsForTag(fs.baseRevision);
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
					Util.debugLog(String.format("ZKFSManager %s: IS rebasing to new revtag %s from %s, latest is %s\n",
							fs.archive.master.getName(),
							Util.formatRevisionTag(revtag),
							Util.formatRevisionTag(fs.baseRevision),
							Util.formatRevisionTag(latest)));
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
		} else {
			Util.debugLog(String.format("ZKFSManager %s: NOT rebasing to new revtag %s from %s, latest is %s (autofollow=%s, !dirty=%s, !alreadyOnLatest=%s, isDescendent=%s)\n",
					fs.archive.master.getName(),
					Util.formatRevisionTag(revtag),
					Util.formatRevisionTag(fs.baseRevision),
					Util.formatRevisionTag(latest),
					autofollow ? "true" : "false",
					!fs.isDirty() ? "true" : "false",
					!fs.baseRevision.equals(latest) ? "true" : "false",
					isDescendent ? "true" : "false"
					));
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

	public void setAutocommitIntervalMs(int autocommitIntervalMs) {
		if(this.autocommitIntervalMs == autocommitIntervalMs) return;
		this.autocommitIntervalMs = autocommitIntervalMs;
		setupAutocommitTimer();
		autosaveIfDesired();
	}
	
	public void setMaxAutocommitIntervalMs(int maxAutocommitIntervalMs) {
		if(this.maxAutocommitIntervalMs == maxAutocommitIntervalMs) return;
		this.maxAutocommitIntervalMs = maxAutocommitIntervalMs;
		setupAutocommitTimer();
		autosaveIfDesired();
	}

	public boolean isAutocommiting() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		if(this.autocommit == autocommit) return;
		this.autocommit = autocommit;
		setupAutocommitTimer();
		autosaveIfDesired();
	}

	public boolean isAutofollowing() {
		return autofollow;
	}

	public void setAutofollow(boolean autofollow) {
		if(this.autofollow == autofollow) return;
		this.autofollow = autofollow;
		autosaveIfDesired();
	}
	
	protected void setupAutocommitTimer() {
		if(!autocommit || autocommitIntervalMs <= 0) {
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
				Util.debugLog(String.format("ZKFS %s %s: IOException performing autocommit on revision %s\n",
						Util.formatArchiveId(fs.archive.config.archiveId),
						fs.archive.master.getName(),
						fs.getBaseRevision()));
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
			Stat stat = mirror.getTarget().stat("/");
			if(!stat.isDirectory()) {
				throw new EISNOTDIRException(this.automirrorPath);
			}
			mirror.startWatch();
		} else {
			mirror.stopWatch();
		}

		this.automirror = automirror;
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
			mirror = new FSMirror(fs, new LocalFS(automirrorPath));
			if(automirror) {
				mirror.startWatch();
			}
		} else {
			mirror = null;
			automirror = false;
		}

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
		if(fs == null) return;
		MutableSecureFile
		  .atPath(fs.getArchive().getConfig().getLocalStorage(),
				path(),
				storageKey(fs.getArchive().getConfig()))
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

		builder.add("autocommit", autocommit);
		builder.add("autofollow", autofollow);
		builder.add("automirror", automirror);
		builder.add("automerge", isAutomerging());
		builder.add("autocommitIntervalMs", autocommitIntervalMs);
		
		builder.add("advertising", fs.getArchive().getConfig().isAdvertising());
		builder.add("requestingAll", fs.getArchive().getConfig().getSwarm().isRequestingAll());
		builder.add("peerLimit", fs.getArchive().getConfig().getSwarm().getMaxSocketCount());
		
		if(automirrorPath != null) {
			builder.add("automirrorPath", automirrorPath);
		}
		builder.add("revtag", Util.encode64(fs.getBaseRevision().serialize()));
		
		return builder.build().toString().getBytes();
	}
	
	protected void deserialize(ZKArchiveConfig config, byte[] serialized) throws IOException {
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		
		String tagRaw = json.getString("revtag");
		if(tagRaw != null) {
			RevisionTag revTag = new RevisionTag(config, Util.decode64(tagRaw), true);
			fs = revTag.getFS();
		} else {
			fs = config.getArchive().openLatest();
		}
		
		setAutocommit(json.getBoolean("autocommit"));
		setAutofollow(json.getBoolean("autofollow"));
		setAutomerge(json.getBoolean("automerge"));
		setAutocommitIntervalMs(json.getInt("autocommitIntervalMs"));
		
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
	}

	public void setFs(ZKFS fs) throws IOException {
		if(fs == this.fs) return;
		
		this.fs.close();
		this.fs = fs;
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
		if(fs.archive.config.getLocalStorage().exists(path())) {
			fs.archive.config.getLocalStorage().unlink(path());
		}
	}

	public boolean isAutomerging() {
		return fs.getArchive().getConfig().getRevisionList().getAutomerge();
	}
	
	public void setAutomerge(boolean automerging) {
		fs.getArchive().getConfig().getRevisionList().setAutomerge(automerging);
	}
	
	public FSMirror getMirror() {
		return mirror;
	}
}
