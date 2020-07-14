package com.acrescrypto.zksyncweb.data;

import java.io.IOException;
import java.util.HashMap;

import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksyncweb.State;

public class XGlobalInfo {
	private Long                   bytesPerSecondTx;
	private Long                   bytesPerSecondRx;
	private Long                   lifetimeBytesTx;
	private Long                   lifetimeBytesRx;
	private Integer                numArchives;
	private HashMap<String,Object> settings;
	private Boolean                isListening;
	private Long                   memoryUsed;
	private Long                   memoryTotal;
	private Long                   memoryMax;
	private Integer                numOpenFileHandles;
	
	public static XGlobalInfo globalInfo() throws IOException {
		ZKMaster    master      = State.sharedState().getMaster();
		XGlobalInfo info        = new XGlobalInfo();
		
		info.bytesPerSecondTx   = master.getBandwidthMonitorTx().getBytesPerSecond();
		info.bytesPerSecondRx   = master.getBandwidthMonitorRx().getBytesPerSecond();
		info.lifetimeBytesTx    = master.getBandwidthMonitorTx().getLifetimeBytes();
		info.lifetimeBytesRx    = master.getBandwidthMonitorRx().getLifetimeBytes();
		info.numArchives        = master.allConfigs().size();
		info.isListening        = master.getTCPListener().isListening();
		info.settings           = master.getGlobalConfig().asHash();
		info.memoryUsed         = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
		info.memoryTotal        = Runtime.getRuntime().totalMemory();
		info.memoryMax          = Runtime.getRuntime().maxMemory();
		info.numOpenFileHandles = FS.fileHandleTelemetryEnabled ? FS.getGlobalOpenFiles().size() : null;
		
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

	public Long getLifetimeBytesTx() {
		return lifetimeBytesTx;
	}

	public void setLifetimeBytesTx(Long lifetimeBytesTx) {
		this.lifetimeBytesTx = lifetimeBytesTx;
	}

	public Long getLifetimeBytesRx() {
		return lifetimeBytesRx;
	}

	public void setLifetimeBytesRx(Long lifetimeBytesRx) {
		this.lifetimeBytesRx = lifetimeBytesRx;
	}

	public Long getMemoryUsed() {
		return memoryUsed;
	}

	public void setMemoryUsed(Long memoryUsed) {
		this.memoryUsed = memoryUsed;
	}

	public Long getMemoryMax() {
		return memoryMax;
	}

	public void setMemoryMax(Long memoryMax) {
		this.memoryMax = memoryMax;
	}

	public Long getMemoryTotal() {
		return memoryTotal;
	}

	public void setMemoryTotal(Long memoryTotal) {
		this.memoryTotal = memoryTotal;
	}

	public Integer getNumOpenFileHandles() {
		return numOpenFileHandles;
	}

	public void setNumOpenFileHandles(Integer openFileHandles) {
		this.numOpenFileHandles = openFileHandles;
	}
}
