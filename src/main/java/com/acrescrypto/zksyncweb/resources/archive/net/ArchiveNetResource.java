package com.acrescrypto.zksyncweb.resources.archive.net;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XArchiveNetInfo;
import com.acrescrypto.zksyncweb.data.XTCPAdListing;

@Path("/archives/{archiveid}/net")
public class ArchiveNetResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getNet(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		throw XAPIResponse.withPayload(new XArchiveNetInfo(config));
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("ad")
	public XAPIResponse getNetAd(@PathParam("archiveid") String archiveId) throws IOException, UnconnectableAdvertisementException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		try {
			XTCPAdListing listing = new XTCPAdListing(config.getSwarm());
			throw XAPIResponse.withPayload(listing);
		} catch(UnconnectableAdvertisementException | NullPointerException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		}
	}
}
