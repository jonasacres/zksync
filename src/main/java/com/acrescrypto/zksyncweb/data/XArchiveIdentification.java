package com.acrescrypto.zksyncweb.data;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;

public class XArchiveIdentification {
	private byte[] archiveId;
	
	private String description;
	private Integer pageSize;

	private Boolean usesWriteKey;
	private Boolean haveWriteKey;
	private Boolean haveReadKey;
	private Boolean ready;

	private Integer connectedPeers;

	private byte[] currentRevTag;
	private String currentTitle;
	
	private Long consumedStorage;
	private Long consumedLocalStorage;
	private Long bytesPerSecondRx;
	private Long bytesPerSecondTx;
	private Long lifetimeBytesRx;
	private Long lifetimeBytesTx;

	private XArchiveSettings config;
	
	public static XArchiveIdentification fromConfig(ZKArchiveConfig config) {
		XArchiveIdentification id = new XArchiveIdentification();
		XArchiveSettings xset;
		
		id.archiveId = config.getArchiveId().clone();
		id.description = config.getDescription();
		id.pageSize = config.getPageSize();
		
		id.usesWriteKey = config.usesWriteKey();
		id.haveWriteKey = !config.isReadOnly();
		id.haveReadKey = !config.getAccessor().isSeedOnly();
		id.ready = config.haveConfigLocally();
		
		id.connectedPeers = config.getSwarm().getConnections().size();
		id.bytesPerSecondRx = config.getSwarm().getBandwidthMonitorRx().getBytesPerSecond();
		id.bytesPerSecondTx = config.getSwarm().getBandwidthMonitorTx().getBytesPerSecond();
		id.lifetimeBytesRx = config.getSwarm().getBandwidthMonitorRx().getLifetimeBytes();
		id.lifetimeBytesTx = config.getSwarm().getBandwidthMonitorTx().getLifetimeBytes();
		
		try {
			id.currentRevTag = State.sharedState().activeFs(config).getBaseRevision().getBytes();
			id.currentTitle = State.sharedState().activeFs(config).getInodeTable().getNextTitle();
		} catch (IOException | NullPointerException exc) {
		}
		
		try {
			xset = XArchiveSettings.fromConfig(config);
			id.consumedStorage = config.getStorage().storageSize("/");
			id.consumedLocalStorage = config.getLocalStorage().storageSize("/");
		} catch(IOException exc) {
			xset = new XArchiveSettings();
			// TODO API: (log) worth logging this...
		}
		
		id.config = xset;
		
		return id;
	}

	public byte[] getArchiveId() {
		return archiveId;
	}

	public void setArchiveId(byte[] archiveId) {
		this.archiveId = archiveId;
	}
	
	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}

	public Integer getPageSize() {
		return pageSize;
	}

	public void setPageSize(Integer pageSize) {
		this.pageSize = pageSize;
	}

	public Boolean isUsesWriteKey() {
		return usesWriteKey;
	}

	public void setUsesWriteKey(Boolean usesWriteKey) {
		this.usesWriteKey = usesWriteKey;
	}

	public Boolean isHaveWriteKey() {
		return haveWriteKey;
	}

	public void setHaveWriteKey(Boolean haveWriteKey) {
		this.haveWriteKey = haveWriteKey;
	}

	public Boolean isHaveReadKey() {
		return haveReadKey;
	}

	public void setHaveReadKey(Boolean haveReadKey) {
		this.haveReadKey = haveReadKey;
	}

	public Integer getConnectedPeers() {
		return connectedPeers;
	}

	public void setConnectedPeers(Integer connectedPeers) {
		this.connectedPeers = connectedPeers;
	}

	public byte[] getCurrentRevTag() {
		return currentRevTag;
	}

	public void setCurrentRevTag(byte[] currentRevTag) {
		this.currentRevTag = currentRevTag;
	}

	public Long getConsumedStorage() {
		return consumedStorage;
	}

	public void setConsumedStorage(Long consumedStorage) {
		this.consumedStorage = consumedStorage;
	}

	public XArchiveSettings getConfig() {
		return config;
	}

	public void setConfig(XArchiveSettings config) {
		this.config = config;
	}

	public Boolean isReady() {
		return ready;
	}

	public void setReady(Boolean ready) {
		this.ready = ready;
	}

	public Long getConsumedLocalStorage() {
		return consumedLocalStorage;
	}

	public void setConsumedLocalStorage(Long consumedLocalStorage) {
		this.consumedLocalStorage = consumedLocalStorage;
	}

	public Long getBytesPerSecondRx() {
		return bytesPerSecondRx;
	}

	public void setBytesPerSecondRx(Long bytesPerSecondRx) {
		this.bytesPerSecondRx = bytesPerSecondRx;
	}

	public Long getBytesPerSecondTx() {
		return bytesPerSecondTx;
	}

	public void setBytesPerSecondTx(Long bytesPerSecondTx) {
		this.bytesPerSecondTx = bytesPerSecondTx;
	}

	public String getCurrentTitle() {
		return currentTitle;
	}

	public void setCurrentTitle(String currentTitle) {
		this.currentTitle = currentTitle;
	}

	public Long getLifetimeBytesRx() {
		return lifetimeBytesRx;
	}

	public void setLifetimeBytesRx(Long lifetimeBytesRx) {
		this.lifetimeBytesRx = lifetimeBytesRx;
	}

	public Long getLifetimeBytesTx() {
		return lifetimeBytesTx;
	}

	public void setLifetimeBytesTx(Long lifetimeBytesTx) {
		this.lifetimeBytesTx = lifetimeBytesTx;
	}
}
