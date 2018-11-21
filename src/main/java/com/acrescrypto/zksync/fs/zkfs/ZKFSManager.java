package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.RevisionList.RevisionMonitor;
import com.acrescrypto.zksync.fs.zkfs.ZKFS.ZKFSDirtyMonitor;
import com.acrescrypto.zksync.utility.SnoozeThread;

public class ZKFSManager {
	protected int autocommitIntervalMs;
	protected boolean autocommit;
	protected boolean autofollow;
	protected ZKFS fs;
	protected SnoozeThread autocommitTimer;
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
		if(autofollow && !fs.isDirty() && !fs.baseRevision.equals(latest)) {
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
}
