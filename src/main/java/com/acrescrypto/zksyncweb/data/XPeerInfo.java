package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.PeerConnection;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.utility.BandwidthMonitor;

public class XPeerInfo {
	public final static int ROLE_REQUESTOR = 0;
	public final static int ROLE_RESPONDER = 1;
	
	private Long bytesPerSecondTx;
	private Long bytesPerSecondRx;
	private Integer role;
	private Integer peerType;
	private XTCPAdListing ad;
	private Boolean localPaused;
	private Boolean remotePaused;
	private Boolean wantsEverything;
	private Long timeStart;
	
	public XPeerInfo() {}
	
	public XPeerInfo(PeerConnection conn) {
		this.bytesPerSecondRx = bytesFromMonitor(conn.getSocket().getMonitorRx());
		this.bytesPerSecondTx = bytesFromMonitor(conn.getSocket().getMonitorTx());
		this.role = conn.getSocket().isLocalRoleClient() ? ROLE_REQUESTOR : ROLE_RESPONDER;
		this.peerType = conn.getPeerType();
		
		if(conn.getSocket().getAd() instanceof TCPPeerAdvertisement) {
			this.ad = new XTCPAdListing((TCPPeerAdvertisement) conn.getSocket().getAd());
		}
		
		this.localPaused = conn.isLocalPaused();
		this.remotePaused = conn.isRemotePaused();
		this.wantsEverything = conn.wantsEverything();
		this.timeStart = conn.getTimeStart();
	}
	
	protected Long bytesFromMonitor(BandwidthMonitor monitor) {
		if(monitor == null) return null;
		return monitor.getBytesPerSecond();
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

	public Integer getRole() {
		return role;
	}

	public void setRole(Integer role) {
		this.role = role;
	}

	public Integer getPeerType() {
		return peerType;
	}

	public void setPeerType(Integer peerType) {
		this.peerType = peerType;
	}

	public XTCPAdListing getAd() {
		return ad;
	}

	public void setAd(XTCPAdListing ad) {
		this.ad = ad;
	}

	public Boolean getLocalPaused() {
		return localPaused;
	}

	public void setLocalPaused(Boolean localPaused) {
		this.localPaused = localPaused;
	}

	public Boolean getRemotePaused() {
		return remotePaused;
	}

	public void setRemotePaused(Boolean remotePaused) {
		this.remotePaused = remotePaused;
	}

	public Boolean getWantsEverything() {
		return wantsEverything;
	}

	public void setWantsEverything(Boolean wantsEverything) {
		this.wantsEverything = wantsEverything;
	}

	public Long getTimeStart() {
		return timeStart;
	}

	public void setTimeStart(Long timeStart) {
		this.timeStart = timeStart;
	}
}
