package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.MutableSecureFile;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.RevisionList.RevisionMonitor;
import com.acrescrypto.zksync.fs.zkfs.ZKFS.ZKFSDirtyMonitor;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.Util;

public class ZKFSManager implements AutoCloseable {
	protected int autocommitIntervalMs;
	protected boolean autocommit;
	protected boolean autofollow;
	protected boolean automirror;
	protected String automirrorPath;
	
	protected ZKFS fs;
	protected SnoozeThread autocommitTimer;
	protected FSMirror mirror;
	
	protected Logger logger = LoggerFactory.getLogger(ZKFSManager.class);
	protected RevisionMonitor revMonitor;
	protected ZKFSDirtyMonitor fsMonitor;
	
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
		this.fsMonitor = (f)->notifyLocalChanges();
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
			fs.close();
		}
		
		if(mirror != null) {
			mirror.stopWatch();
		}
	}
	
	public void notifyLocalChanges() {
		if(autocommitTimer != null && !autocommitTimer.isExpired()) {
			autocommitTimer.snooze();
		}
	}
	
	public void notifyNewRevtag(RevisionTag revtag) {
		RevisionTag latest = fs.archive.config.revisionList.latest();
		boolean isDescendent = false;
		try {
			isDescendent = fs.archive.config.revisionTree.descendentOf(latest, fs.baseRevision);
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
	}
	
	public ZKFS getFs() {
		return fs;
	}

	public int getAutocommitIntervalMs() {
		return autocommitIntervalMs;
	}

	public void setAutocommitIntervalMs(int autocommitIntervalMs) {
		if(this.autocommitIntervalMs == autocommitIntervalMs) return;
		this.autocommitIntervalMs = autocommitIntervalMs;
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
			
			return;
		}
		
		if(autocommitTimer != null) {
			autocommitTimer.cancel();
		}
		
		autocommitTimer = new SnoozeThread(autocommitIntervalMs, false, ()->{
			try {
				if(fs.isDirty()) {
					fs.commit();
				}
				
				setupAutocommitTimer();
			} catch (IOException exc) {
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
		this.fs.close();
		this.fs = fs;
		setupMonitors();
		setAutomirrorPath(this.automirrorPath); // reinit mirror
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
}
