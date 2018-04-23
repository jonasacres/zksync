package com.acrescrypto.zksync.net;

import java.util.ArrayList;
import java.util.HashMap;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.net.PeerConnection.PeerConnectionDelegate;

public class PeerController implements PeerConnectionDelegate {
	protected PeerSwarm swarm;
	protected ZKArchive archive;
	
	protected HashMap<String,ArrayList<PeerControllerCallback>> callbacks = new HashMap<String,ArrayList<PeerControllerCallback>>();
	
	public interface PeerControllerCallback {
		public void received();
	}
	
	public void requestRefTag(RefTag tag, PeerControllerCallback cb) {
		addCallback(tag.getBytes(), cb);
		// divide the work between multiple peers: 16 pages from each peer, then reapportion on completion
		// when no further pages to allocate, reallocate remaining pages from slowest to fastest 
	}
	
	public void requestPage(byte[] tag, PeerControllerCallback cb) {
		addCallback(tag, cb);
		// select a random peer to send us the page (with timeout and cycle for handling bad/slow peers)
	}
	
	public void requestTips(PeerControllerCallback cb) {
		// how to do this? should keep track of tips hash... broadcast hash to everyone, get responses from those who differ
		// cache hashes of files we've obsoleted/merged...
	}
	
	public void announceTip(RefTag tag) {
	}
	
	public void addCallback(byte[] identifier, PeerControllerCallback cb) {
		String hex = Util.bytesToHex(identifier);
		callbacks.putIfAbsent(hex, new ArrayList<PeerControllerCallback>());
		callbacks.get(hex).add(cb);
	}
	
	public void runCallbacks(byte[] identifier) {
		String hex = Util.bytesToHex(identifier);
		if(!callbacks.containsKey(hex)) return;
		for(PeerControllerCallback cb : callbacks.get(hex)) {
			cb.received();
		}
		callbacks.remove(hex);
	}
	
	@Override
	public void discoveredTip(PeerConnection conn, RefTag tag) {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void receivedPage(PeerConnection conn, byte[] tag) {
		runCallbacks(tag);
		// TODO Auto-generated method stub
	}
	
	@Override
	public void receivedTipFile(PeerConnection conn, byte[] contents) {
	}

	@Override
	public void receivedRefTag(PeerConnection conn, RefTag tag) {
		runCallbacks(tag.getBytes());
	}
	
	
}
