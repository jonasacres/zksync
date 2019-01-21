package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.Blacklist;

public class XBlacklistConfig {
	private Boolean enabled;
	
	public XBlacklistConfig(Blacklist blacklist) {
		this.enabled = blacklist.isEnabled();
	}

	public XBlacklistConfig() {}

	public Boolean isEnabled() {
		return enabled;
	}

	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}
}
