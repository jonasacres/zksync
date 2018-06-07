package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.net.PeerConnection.PeerCapabilityException;
import com.acrescrypto.zksync.utility.Util;

public class RequestPool {
	public final static int DEFAULT_PRUNE_INTERVAL_MS = 30*1000;
	public static int pruneIntervalMs = DEFAULT_PRUNE_INTERVAL_MS;
	
	ZKArchive archive;
	LinkedList<RefTag> requestedRefTags = new LinkedList<>();
	LinkedList<RefTag> requestedRevisions = new LinkedList<>();
	LinkedList<Long> requestedPageTags = new LinkedList<>();
	
	boolean requestingEverything, stopped;
	Logger logger = LoggerFactory.getLogger(RequestPool.class);
	
	public RequestPool(ZKArchive archive) {
		this.archive = archive;
		new Thread(()->pruneThread()).start();
	}
	
	public void stop() {
		stopped = true;
	}
	
	public void setRequestingEverything(boolean requestingEverything) {
		this.requestingEverything = requestingEverything;
	}
	
	public synchronized void addRequestsToConnection(PeerConnection conn) {
		if(requestingEverything) {
			conn.requestAll();
		}
		
		conn.requestPageTags(requestedPageTags);
		try {
			conn.requestRefTags(requestedRefTags);
			conn.requestRevisionContents(requestedRevisions);
		} catch(PeerCapabilityException exc) {}
	}
	
	public synchronized void addRefTag(RefTag refTag) {
		requestedRefTags.add(refTag);
	}
	
	public synchronized void addRevision(RefTag revTag) {
		requestedRevisions.add(revTag);
	}
	
	public synchronized void addPageTag(long shortTag) {
		requestedPageTags.add(shortTag);
	}
	
	public synchronized void addPageTag(byte[] pageTag) {
		requestedPageTags.add(Util.shortTag(pageTag));
	}
	
	public synchronized void prune() {
		try {
			prunePageTags();
			pruneRefTags();
			pruneRevisionTags();
		} catch(IOException exc) {
			logger.error("Caught exception pruning request pool", exc);
		}
	}
	
	protected void pruneThread() {
		while(!stopped) {
			try {
				Util.sleep(pruneIntervalMs);
				prune();
			} catch(Exception exc) {
				logger.error("Prune thread caught exception", exc);
			}
		}
	}
	
	protected void prunePageTags() throws IOException {
		LinkedList<Long> toRemove = new LinkedList<>();
		for(long shortTag : requestedPageTags) {
			byte[] tag = archive.expandShortTag(shortTag);
			if(tag != null) {
				toRemove.add(shortTag);
			}
		}
		
		for(Long shortTag : toRemove) {
			requestedPageTags.remove(shortTag);
		}
	}
	
	protected void pruneRefTags() throws IOException {
		LinkedList<RefTag> toRemove = new LinkedList<>();
		for(RefTag refTag : requestedRefTags) {
			if(archive.hasRefTag(refTag)) {
				toRemove.add(refTag);
			}
		}
		
		for(RefTag refTag : toRemove) {
			requestedRefTags.remove(refTag);
		}
	}

	protected void pruneRevisionTags() throws IOException {
		LinkedList<RefTag> toRemove = new LinkedList<>();
		for(RefTag revTag : requestedRevisions) {
			if(archive.hasRefTag(revTag)) {
				toRemove.add(revTag);
			}
		}
		
		for(RefTag revTag : toRemove) {
			requestedRevisions.remove(revTag);
		}
	}
}
