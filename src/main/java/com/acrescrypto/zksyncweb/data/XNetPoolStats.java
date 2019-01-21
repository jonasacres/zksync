package com.acrescrypto.zksyncweb.data;

import com.acrescrypto.zksync.net.RequestPool;

public class XNetPoolStats {
	private Integer numPages;
	private Integer numInodes;
	private Integer numRevisions;
	private Integer numRevisionDetails;
	private Boolean isRequestingEverything;
	private Boolean paused;
	
	public XNetPoolStats() {}
	
	public XNetPoolStats(RequestPool pool) {
		this.numPages = pool.numPagesRequested();
		this.numInodes = pool.numInodesRequested();
		this.numRevisions = pool.numRevisionsRequested();
		this.numRevisionDetails = pool.numRevisionDetailsRequested();
		this.isRequestingEverything = pool.isRequestingEverything();
		this.paused = pool.isPaused();
	}
	
	public Integer getNumInodes() {
		return numInodes;
	}

	public void setNumInodes(Integer numInodes) {
		this.numInodes = numInodes;
	}

	public Integer getNumPages() {
		return numPages;
	}

	public void setNumPages(Integer numPages) {
		this.numPages = numPages;
	}

	public Integer getNumRevisionDetails() {
		return numRevisionDetails;
	}
	
	public void setNumRevisionDetails(Integer numRevisionDetails) {
		this.numRevisionDetails = numRevisionDetails;
	}
	
	public Integer getNumRevisions() {
		return numRevisions;
	}
	
	public void setNumRevisions(Integer numRevisions) {
		this.numRevisions = numRevisions;
	}

	public Boolean getPaused() {
		return paused;
	}
	
	public void setPaused(Boolean paused) {
		this.paused = paused;
	}

	public Boolean getIsRequestingEverything() {
		return isRequestingEverything;
	}

	public void setIsRequestingEverything(Boolean isRequestingEverything) {
		this.isRequestingEverything = isRequestingEverything;
	}
}
