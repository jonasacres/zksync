package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;

public class XArchiveNetInfo {
	private int numPeers;
	private int numKnownAds;
	private int numConnectedAds;
	private int numEmbargoedAds;
	private byte[] staticPubKey;
	
	private Long bytesPerSecondTx;
	private Long bytesPerSecondRx;
	
	private Long maxBytesPerSecondTx;
	private Long maxBytesPerSecondRx;
	
	public XArchiveNetInfo() {}
	
	public XArchiveNetInfo(ZKArchiveConfig config) {
		this.numPeers = config.getSwarm().numConnections();
		this.staticPubKey = config.getSwarm().getPublicIdentityKey().getBytes();
		this.numKnownAds = config.getSwarm().numKnownAds();
		this.numConnectedAds = config.getSwarm().numConnectedAds();
		this.numEmbargoedAds = config.getSwarm().numEmbargoedAds();
		
		this.bytesPerSecondTx = config.getSwarm().getBandwidthMonitorTx().getBytesPerSecond();
		this.bytesPerSecondRx = config.getSwarm().getBandwidthMonitorRx().getBytesPerSecond();
		
		this.maxBytesPerSecondTx = config.getSwarm().getBandwidthAllocatorTx().getBytesPerSecond();
		this.maxBytesPerSecondRx = config.getSwarm().getBandwidthAllocatorRx().getBytesPerSecond();
	}
	
	public Integer getNumPeers() {
		return numPeers;
	}
	
	public void setNumPeers(int numPeers) {
		this.numPeers = numPeers;
	}

	public byte[] getStaticPubKey() {
		return staticPubKey;
	}

	public void setStaticPubKey(byte[] staticPubKey) {
		this.staticPubKey = staticPubKey;
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

	public Long getMaxBytesPerSecondTx() {
		return maxBytesPerSecondTx;
	}

	public void setMaxBytesPerSecondTx(Long maxBytesPerSecondTx) {
		this.maxBytesPerSecondTx = maxBytesPerSecondTx;
	}

	public Long getMaxBytesPerSecondRx() {
		return maxBytesPerSecondRx;
	}

	public void setMaxBytesPerSecondRx(Long maxBytesPerSecondRx) {
		this.maxBytesPerSecondRx = maxBytesPerSecondRx;
	}

	public int getNumKnownAds() {
		return numKnownAds;
	}

	public void setNumKnownAds(int numKnownAds) {
		this.numKnownAds = numKnownAds;
	}

	public int getNumConnectedAds() {
		return numConnectedAds;
	}

	public void setNumConnectedAds(int numConnectedAds) {
		this.numConnectedAds = numConnectedAds;
	}

	public int getNumEmbargoedAds() {
		return numEmbargoedAds;
	}

	public void setNumEmbargoedAds(int numEmbargoedAds) {
		this.numEmbargoedAds = numEmbargoedAds;
	}
}
