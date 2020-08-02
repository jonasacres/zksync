package com.acrescrypto.zksyncweb.data;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.net.PeerConnection;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.State;

public class XPeerInfo {
	public final static int ROLE_REQUESTOR = 0;
	public final static int ROLE_RESPONDER = 1;
	
	private Long bytesPerSecondTx;
	private Long bytesPerSecondRx;
	private Long lifetimeBytesTx;
	private Long lifetimeBytesRx;
	private Integer numAnnouncedTags;
	private Integer role;
	private Integer peerType;
	private XTCPAdListing ad;
	private Boolean localPaused;
	private Boolean remotePaused;
	private Boolean wantsEverything;
	private Long timeStart;
	private String connectionId;
	
	protected final Logger logger = LoggerFactory.getLogger(XPeerInfo.class);
	
	public XPeerInfo() {}
	
	public XPeerInfo(PeerConnection conn) {
		this.numAnnouncedTags = conn.announcedTags().size();
		this.bytesPerSecondRx = bytesFromMonitor(conn.getSocket().getMonitorRx());
		this.bytesPerSecondTx = bytesFromMonitor(conn.getSocket().getMonitorTx());
		this.lifetimeBytesRx = lifetimeBytesFromMonitor(conn.getSocket().getMonitorRx());
		this.lifetimeBytesTx = lifetimeBytesFromMonitor(conn.getSocket().getMonitorTx());
		this.role = conn.getSocket().isLocalRoleClient() ? ROLE_REQUESTOR : ROLE_RESPONDER;
		this.peerType = conn.getPeerType();
		try {
			this.connectionId = Util.bytesToHex(State.sharedCrypto().authenticate(
					conn.getSocket().getSwarm().getPublicIdentityKey().getBytes(),
					Util.serializeInt(System.identityHashCode(conn))
					), 8);
		} catch (IOException exc) {
			logger.error("XPeerInfo caught exception serializing connection ID", exc);
		}
		
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
	
	protected Long lifetimeBytesFromMonitor(BandwidthMonitor monitor) {
		if(monitor == null) return null;
		return monitor.getLifetimeBytes();
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

	public String getConnectionId() {
		return connectionId;
	}

	public void setConnectionId(String connectionId) {
		this.connectionId = connectionId;
	}
	
	public Integer getNumAnnouncedTags() {
		return numAnnouncedTags;
	}
	
	public void setNumAnnouncedTags(Integer numAnnouncedTags) {
		this.numAnnouncedTags = numAnnouncedTags;
	}
}
