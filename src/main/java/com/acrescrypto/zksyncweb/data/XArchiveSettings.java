package com.acrescrypto.zksyncweb.data;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFSManager;
import com.acrescrypto.zksyncweb.State;

public class XArchiveSettings {
	private Boolean advertising;
	private Boolean requestingAll;
	private Boolean autocommit;
	private Boolean autofollow;
	private Boolean automirror;
	
	private Integer peerLimit;
	private Integer autocommitInterval;
	
	private String automirrorPath;
	
	public static XArchiveSettings fromConfig(ZKArchiveConfig config) throws IOException {
		XArchiveSettings settings = new XArchiveSettings();
		ZKFSManager manager = State.sharedState().activeManager(config);
		settings.advertising = config.isAdvertising();
		settings.requestingAll = config.getSwarm().isRequestingAll();
		settings.peerLimit = config.getSwarm().getMaxSocketCount();
		if(!config.getAccessor().isSeedOnly() && manager != null) {
			settings.autocommit = manager.isAutocommiting();
			settings.autocommitInterval = manager.getAutocommitIntervalMs();
			settings.autofollow = manager.isAutofollowing();
			settings.automirror = manager.isAutomirroring();
			settings.automirrorPath = manager.getAutomirrorPath();

		}
		return settings;
	}
	
	public Boolean isAdvertising() {
		return advertising;
	}
	
	public void setAdvertising(Boolean advertising) {
		this.advertising = advertising;
	}
	
	public Boolean isRequestingAll() {
		return requestingAll;
	}
	
	public void setRequestingAll(Boolean requestingAll) {
		this.requestingAll = requestingAll;
	}
	
	public Boolean isAutocommit() {
		return autocommit;
	}
	
	public void setAutocommiting(Boolean autocommit) {
		this.autocommit = autocommit;
	}
	
	public Boolean isAutofollow() {
		return autofollow;
	}
	
	public void setAutofollow(Boolean autofollow) {
		this.autofollow = autofollow;
	}
	
	public Integer getPeerLimit() {
		return peerLimit;
	}
	
	public void setPeerLimit(Integer peerLimit) {
		this.peerLimit = peerLimit;
	}
	
	public Integer getAutocommitInterval() {
		return autocommitInterval;
	}
	
	public void setAutocommitInterval(Integer autocommitInterval) {
		this.autocommitInterval = autocommitInterval;
	}

	public String getAutomirrorPath() {
		return automirrorPath;
	}

	public void setAutomirrorPath(String automirrorPath) {
		this.automirrorPath = automirrorPath;
	}

	public Boolean isAutomirror() {
		return automirror;
	}

	public void setAutomirror(Boolean automirror) {
		this.automirror = automirror;
	}
}
