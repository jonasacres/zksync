package com.acrescrypto.zksyncweb.data;

import java.util.LinkedList;
import java.util.List;
import com.acrescrypto.zksync.net.dht.*;

public class XDHTPeerFile {
	private   byte[]                  networkId;
	private   List<XDHTPeerFileEntry> peers;
	
	public XDHTPeerFile() {}
	
	public XDHTPeerFile(DHTClient client) {
		this.networkId = client.getNetworkId();
		peers = new LinkedList<>();
		
		XDHTPeerFileEntry localEntry = new XDHTPeerFileEntry(client.getProtocolManager().getLocalPeer());
		peers.add(localEntry);
		
		for(DHTPeer peer : client.getRoutingTable().allPeers()) {
			if(peer.isBad())          continue;
			if(peer.isQuestionable()) continue;
			
			XDHTPeerFileEntry entry = new XDHTPeerFileEntry(peer);
			peers.add(entry);
		}
	}
	
	public byte[] getNetworkId() {
		return networkId;
	}
	
	public void setNetworkId(byte[] networkId) {
		this.networkId = networkId;
	}
	
	public List<XDHTPeerFileEntry> getPeers() {
		return peers;
	}
	
	public void setPeers(List<XDHTPeerFileEntry> peers) {
		this.peers = peers;
	}
}
