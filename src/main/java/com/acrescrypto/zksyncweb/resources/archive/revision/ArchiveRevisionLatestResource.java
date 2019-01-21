package com.acrescrypto.zksyncweb.resources.archive.revision;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XRevisionInfo;

@Path("/archives/{archiveId}/revisions/latest")
public class ArchiveRevisionLatestResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getLatest(@PathParam("archiveId") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		XRevisionInfo xInfo = new XRevisionInfo(config.getRevisionList().latest());
		return XAPIResponse.withPayload(xInfo);
	}
}
