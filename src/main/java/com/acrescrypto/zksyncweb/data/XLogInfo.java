package com.acrescrypto.zksyncweb.data;

import java.util.List;

import com.acrescrypto.zksync.utility.MemLogAppender.LogEvent;
import com.acrescrypto.zksync.utility.Util;

public class XLogInfo {
	private List<LogEvent> entries;
	private Long launchTime;
	
	public XLogInfo(List<LogEvent> entries) {
		this.entries = entries;
		this.launchTime = Util.launchTime();
	}
	
	public List<LogEvent> getEntries() {
		return entries;
	}
	
	public void setEntries(List<LogEvent> entries) {
		this.entries = entries;
	}

	public Long getLaunchTime() {
		return launchTime;
	}

	public void setLaunchTime(Long timestamp) {
		this.launchTime = timestamp;
	}
}
