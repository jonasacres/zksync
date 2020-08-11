package com.acrescrypto.zksyncweb.data;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;

public class XArchiveIdentification {
	private byte[] archiveId;
	private byte[] listenStaticPubkey;
	
	private String description;
	private Integer pageSize;
	private Integer numLocalTags;

	private Boolean usesWriteKey;
	private Boolean haveWriteKey;
	private Boolean haveReadKey;
	private Boolean ready;
	private Boolean dirty;

	private Integer connectedPeers;

	private XRevisionInfo currentRevTag;
	private String currentTitle;
	
	private Long consumedPageStorage;
	private Long consumedLocalStorage;
	private Long bytesPerSecondRx;
	private Long bytesPerSecondTx;
	private Long lifetimeBytesRx;
	private Long lifetimeBytesTx;

	private XArchiveSettings config;
	
	public static XArchiveIdentification fromConfig(ZKArchiveConfig config) throws IOException {
		XArchiveIdentification id = new XArchiveIdentification();
		XArchiveSettings xset;
		
		id.archiveId          =  config.getArchiveId().clone();
		id.description        =  config.getDescription();
		id.pageSize           =  config.getPageSize();
		
		id.usesWriteKey       =  config.usesWriteKey();
		id.haveWriteKey       = !config.isReadOnly();
		id.haveReadKey        = !config.getAccessor().isSeedOnly();
		id.ready              =  config.haveConfigLocally();
		
		id.listenStaticPubkey = config.getSwarm().getPublicIdentityKey().getBytes();
		
		try {
			id.dirty = State.sharedState().activeFs(config).isDirty();
		} catch (Throwable exc) {}
		
		try {
			id.numLocalTags   = config.getArchive().pageTagList().allPageTags().size();
		} catch(Throwable exc) {}
		
		id.connectedPeers     = config.getSwarm().getConnections().size();
		id.bytesPerSecondRx   = config.getSwarm().getBandwidthMonitorRx().getBytesPerSecond();
		id.bytesPerSecondTx   = config.getSwarm().getBandwidthMonitorTx().getBytesPerSecond();
		id.lifetimeBytesRx    = config.getSwarm().getBandwidthMonitorRx().getLifetimeBytes();
		id.lifetimeBytesTx    = config.getSwarm().getBandwidthMonitorTx().getLifetimeBytes();
		
		try {
			id.currentRevTag  = new XRevisionInfo(State.sharedState().activeFs(config).getBaseRevision(), 1);
			id.currentTitle   = State.sharedState().activeFs(config).getInodeTable().getNextTitle();
		} catch (IOException | NullPointerException exc) {
		}
		
		try {
			xset = XArchiveSettings.fromConfig(config);
			if(config.getArchive() != null) {
    			id.consumedPageStorage      = config.getArchive().pageTagList().storedPageSize();
			}
			
			id.consumedLocalStorage     = config.getLocalStorage().storageSize();
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

	public XRevisionInfo getCurrentRevTag() {
		return currentRevTag;
	}

	public void setCurrentRevTag(XRevisionInfo currentRevTag) {
		this.currentRevTag = currentRevTag;
	}

	public Long getConsumedPageStorage() {
		return consumedPageStorage;
	}

	public void setConsumedPageStorage(Long consumedStorage) {
		this.consumedPageStorage = consumedStorage;
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

	public Boolean getDirty() {
		return dirty;
	}

	public void setDirty(Boolean dirty) {
		this.dirty = dirty;
	}
	
	public Integer getNumLocalTags() {
		return numLocalTags;
	}
	
	public void setNumLocalTags(Integer numLocalTags) {
		this.numLocalTags = numLocalTags;
	}
	
	public byte[] getListenStaticPubkey() {
	    return listenStaticPubkey;
	}
	
	public void setListenStaticPubkey(byte[] listenStaticPubkey) {
	    this.listenStaticPubkey = listenStaticPubkey;
	}
}
