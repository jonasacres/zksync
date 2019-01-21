package com.acrescrypto.zksyncweb.data;

import java.io.IOException;
import java.util.HashMap;

import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksyncweb.State;

public class XGlobalInfo {
	private Long bytesPerSecondTx;
	private Long bytesPerSecondRx;
	private Integer numArchives;
	private HashMap<String,Object> settings;
	private Boolean isListening;
	
	public static XGlobalInfo globalInfo() throws IOException {
		ZKMaster master = State.sharedState().getMaster();
		
		XGlobalInfo info = new XGlobalInfo();
		info.bytesPerSecondTx = master.getBandwidthMonitorTx().getBytesPerSecond();
		info.bytesPerSecondRx = master.getBandwidthMonitorRx().getBytesPerSecond();
		info.numArchives = master.allConfigs().size();
		info.setIsListening(master.getTCPListener().isListening());
		info.settings = master.getGlobalConfig().asHash();
		
		return info;
	}
	
	public Long getBytesPerSecondTx() {
		return bytesPerSecondTx;
	}
	
	public void setBytesPerSecondTx(Long bytesPerSecondTx) {
		this.bytesPerSecondTx = bytesPerSecondTx;
	}

	public Long getBytesPerSecondRx() {
		return bytesPerSecondRx;
	}

	public void setBytesPerSecondRx(Long bytesPerSecondRx) {
		this.bytesPerSecondRx = bytesPerSecondRx;
	}

	public Integer getNumArchives() {
		return numArchives;
	}

	public void setNumArchives(Integer numArchives) {
		this.numArchives = numArchives;
	}

	public HashMap<String,Object> getSettings() {
		return settings;
	}

	public void setSettings(HashMap<String,Object> settings) {
		this.settings = settings;
	}

	public Boolean getIsListening() {
		return isListening;
	}

	public void setIsListening(Boolean isListening) {
		this.isListening = isListening;
	}
}
