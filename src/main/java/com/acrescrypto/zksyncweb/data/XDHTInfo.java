package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;

public class XDHTInfo {
	private byte[] peerId;
	private byte[] pubKey;
	private byte[] networkId;
	
	private String bindAddress;
	private Integer udpPort;
	private Integer status;
	private Integer numRecords;
	private Integer numRecordIds;
	
	private Boolean closed;
	private Boolean initialized;
	private Boolean enabled;
	private Boolean paused;
	
	private Integer numPeers;
	private Integer numGoodPeers;
	private Integer numQuestionablePeers;
	private Integer numBadPeers;
	private Integer numPendingRequests;
	
	private Long bytesPerSecondRx;
	private Long bytesPerSecondTx;
	
	private Long lifetimeBytesTx;
	private Long lifetimeBytesRx;
	
	public XDHTInfo(DHTClient client) {
		numBadPeers = 0;
		numQuestionablePeers = 0;
		numGoodPeers = 0;
		
		this.peerId = client.getId().serialize();
		this.pubKey = client.getPublicKey().getBytes();
		this.networkId = client.getNetworkId();
		
		this.bindAddress = client.getBindAddress();
		this.udpPort = client.getPort();
		this.status = client.getStatus();
		
		this.closed = client.isClosed();
		this.initialized = client.isInitialized();
		this.enabled = client.isEnabled();
		this.setPaused(client.isPaused());
		
		this.numPeers = client.getRoutingTable().allPeers().size();
		for(DHTPeer peer : client.getRoutingTable().allPeers()) {
			if(peer.isBad()) {
				numBadPeers++;
			} else if(peer.isQuestionable()) {
				numQuestionablePeers++;
			} else {
				numGoodPeers++;
			}
		}
		
		this.numPendingRequests = client.numPendingRequests();
		this.bytesPerSecondRx = client.getMonitorRx().getBytesPerSecond();
		this.bytesPerSecondTx = client.getMonitorTx().getBytesPerSecond();
		
		this.lifetimeBytesRx = client.getMonitorRx().getLifetimeBytes();
		this.lifetimeBytesTx = client.getMonitorTx().getLifetimeBytes();
		
		this.numRecordIds = client.getRecordStore().numIds();
		this.numRecords = client.getRecordStore().numRecords();
	}
	
	public byte[] getPeerId() {
		return peerId;
	}
	
	public void setPeerId(byte[] peerId) {
		this.peerId = peerId;
	}
	
	public byte[] getPubKey() {
		return pubKey;
	}
	
	public void setPubKey(byte[] pubKey) {
		this.pubKey = pubKey;
	}
	
	public byte[] getNetworkId() {
		return networkId;
	}
	
	public void setNetworkId(byte[] networkId) {
		this.networkId = networkId;
	}
	
	public String getBindAddress() {
		return bindAddress;
	}
	
	public void setBindAddress(String bindAddress) {
		this.bindAddress = bindAddress;
	}

	public Integer getUdpPort() {
		return udpPort;
	}

	public void setUdpPort(Integer udpPort) {
		this.udpPort = udpPort;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}

	public Boolean getClosed() {
		return closed;
	}

	public void setClosed(Boolean closed) {
		this.closed = closed;
	}

	public Boolean getInitialized() {
		return initialized;
	}

	public void setInitialized(Boolean initialized) {
		this.initialized = initialized;
	}

	public Integer getNumPeers() {
		return numPeers;
	}

	public void setNumPeers(Integer numPeers) {
		this.numPeers = numPeers;
	}

	public Integer getNumGoodPeers() {
		return numGoodPeers;
	}

	public void setNumGoodPeers(Integer numGoodPeers) {
		this.numGoodPeers = numGoodPeers;
	}

	public Integer getNumQuestionablePeers() {
		return numQuestionablePeers;
	}

	public void setNumQuestionablePeers(Integer numQuestionablePeers) {
		this.numQuestionablePeers = numQuestionablePeers;
	}

	public Integer getNumBadPeers() {
		return numBadPeers;
	}

	public void setNumBadPeers(Integer numBadPeers) {
		this.numBadPeers = numBadPeers;
	}

	public Integer getNumPendingRequests() {
		return numPendingRequests;
	}

	public void setNumPendingRequests(Integer numPendingRequests) {
		this.numPendingRequests = numPendingRequests;
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

	public Integer getNumRecords() {
		return numRecords;
	}

	public void setNumRecords(Integer numRecords) {
		this.numRecords = numRecords;
	}

	public Integer getNumRecordIds() {
		return numRecordIds;
	}

	public void setNumRecordIds(Integer numRecordIds) {
		this.numRecordIds = numRecordIds;
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

	public Boolean getEnabled() {
		return enabled;
	}

	public void setEnabled(Boolean enabled) {
		this.enabled = enabled;
	}

	public Boolean getPaused() {
		return paused;
	}

	public void setPaused(Boolean paused) {
		this.paused = paused;
	}
}
