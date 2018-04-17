package com.acrescrypto.zksync.net;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;

public class PeerSwarm {
	protected ArrayList<PeerConnection> connections = new ArrayList<PeerConnection>();
	protected HashSet<String> knownPeers = new HashSet<String>();
	protected HashSet<String> connectedAddresses = new HashSet<String>();
	protected ArrayList<PeerDiscoveryApparatus> discoveryApparatuses = new ArrayList<PeerDiscoveryApparatus>(); // It's "apparatuses." I looked it up.
	protected ZKArchive archive;
	
	int maxSocketCount = 128;
	int maxPeerListSize = 1024;
	
	public PeerSwarm(ZKArchive archive) {
		this.archive = archive;
	}
	
	public synchronized void openedConnection(PeerConnection connection) {
		connectedAddresses.add(connection.socket.getAddress());
		knownPeers.add(connection.socket.getAddress());
		connections.add(connection);
	}
	
	public synchronized void addPeer(String address) {
		if(knownPeers.size() >= maxPeerListSize) return;
		if(!PeerSocket.addressSupported(address)) return;
		
		knownPeers.add(address);
	}
	
	public synchronized void addApparatus(PeerDiscoveryApparatus apparatus) {
		discoveryApparatuses.add(apparatus);
	}
	
	public void connectionThread() {
		new Thread(() -> {
			while(true) {
				String addr = selectConnectionAddress();
				try {
					if(addr == null || connections.size() >= maxSocketCount) {
						TimeUnit.MILLISECONDS.sleep(100);
						continue;
					}
				} catch(InterruptedException exc) {}
				
				try {
					openConnection(addr);
				} catch(UnsupportedProtocolException exc) {
					connectionFailed(addr);
				}
			}
		}).start();
	}
	
	public void discoveryThread() {
		new Thread(() -> {
			while(true) {
				for(PeerDiscoveryApparatus apparatus : discoveryApparatuses) {
					for(String address : apparatus.discoveredPeers(archive)) {
						addPeer(address);
					}
				}
				
				try {
					TimeUnit.MILLISECONDS.sleep(100);
				} catch (InterruptedException e) {}
			}
		}).start();
	}
	
	public synchronized String selectConnectionAddress() {
		for(String peer : knownPeers) {
			if(connectedAddresses.contains(peer)) continue;
			return peer;
		}
		
		return null;
	}
	
	public synchronized void openConnection(String address) throws UnsupportedProtocolException {
		connections.add(new PeerConnection(this, address));
	}
	
	public synchronized void connectionFailed(String address) {
		connectedAddresses.remove(address);
	}
}
