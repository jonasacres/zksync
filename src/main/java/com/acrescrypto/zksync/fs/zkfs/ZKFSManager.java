package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.SnoozeThread;

public class ZKFSManager {
	private int autocommitInterval;
	private boolean autocommit;
	private boolean autofollow;
	private ZKFS fs;
	private SnoozeThread autocommitTimer;
	private Logger logger = LoggerFactory.getLogger(ZKFSManager.class);
	
	public ZKFSManager(ZKFS fs) {
		this.fs = fs;
	}
	
	public void notifyLocalChanges() {
		if(autocommitTimer != null && !autocommitTimer.isExpired()) {
			autocommitTimer.snooze();
		}
	}
	
	public void notifyNewRevtag(RevisionTag revtag) {
		fs.rebase(fs.archive.config.revisionList.latest());
	}

	public int getAutocommitInterval() {
		return autocommitInterval;
	}

	public void setAutocommitInterval(int autocommitInterval) {
		if(this.autocommitInterval == autocommitInterval) return;
		this.autocommitInterval = autocommitInterval;
		setupAutocommitTimer();
		
	}

	public boolean isAutocommit() {
		return autocommit;
	}

	public void setAutocommit(boolean autocommit) {
		if(this.autocommit == autocommit) return;
		this.autocommit = autocommit;
		setupAutocommitTimer();
	}

	public boolean isAutofollow() {
		return autofollow;
	}

	public void setAutofollow(boolean autofollow) {
		this.autofollow = autofollow;
	}
	
	protected void setupAutocommitTimer() {
		if(!autocommit || autocommitInterval <= 0) {
			if(autocommitTimer != null) {
				autocommitTimer.cancel();
				autocommitTimer = null;
			}
			
			return;
		}
		
		autocommitTimer = new SnoozeThread(autocommitInterval, false, ()->{
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
