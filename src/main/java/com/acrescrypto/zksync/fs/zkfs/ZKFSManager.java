package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.localfs.LocalFS;
import com.acrescrypto.zksync.fs.zkfs.RevisionList.RevisionMonitor;
import com.acrescrypto.zksync.fs.zkfs.ZKFS.ZKFSDirtyMonitor;
import com.acrescrypto.zksync.utility.SnoozeThread;

public class ZKFSManager {
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
	
	public ZKFSManager(ZKFS fs) {
		this.fs = fs;
		this.fsMonitor = (f)->notifyLocalChanges();
		this.revMonitor = (revTag)->notifyNewRevtag(revTag);
		
		fs.addMonitor(fsMonitor);
		fs.getArchive().getConfig().getRevisionList().addMonitor(revMonitor);
	}
	
	public ZKFSManager(ZKFS fs, ZKFSManager manager) throws IOException {
		this(fs);
		
		setAutocommit(manager.autocommit);
		setAutofollow(manager.autofollow);
		setAutocommitIntervalMs(manager.autocommitIntervalMs);
		setAutomirrorPath(manager.automirrorPath);
		setAutomirror(manager.automirror);
	}

	public void close() {
		fs.removeMonitor(fsMonitor);
		fs.getArchive().getConfig().getRevisionList().removeMonitor(revMonitor);
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
				fs.rebase(latest);
			} catch (IOException exc) {
				logger.error("Unable to rebase to revtag", exc);
			}
		}
	}

	public int getAutocommitIntervalMs() {
		return autocommitIntervalMs;
	}

	public void setAutocommitIntervalMs(int autocommitIntervalMs) {
		if(this.autocommitIntervalMs == autocommitIntervalMs) return;
		this.autocommitIntervalMs = autocommitIntervalMs;
		setupAutocommitTimer();
		
	}

	public boolean isAutocommiting() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		if(this.autocommit == autocommit) return;
		this.autocommit = autocommit;
		setupAutocommitTimer();
	}

	public boolean isAutofollowing() {
		return autofollow;
	}

	public void setAutofollow(boolean autofollow) {
		this.autofollow = autofollow;
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
				logger.error("IOException performing autocommit", exc);
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

		this.automirror = automirror;
		if(automirror && this.automirrorPath != null) {
			mirror.startWatch();
		} else {
			mirror.stopWatch();
		}
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
			automirror = false;
		}
	}
}
