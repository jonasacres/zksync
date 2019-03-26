package com.acrescrypto.zksyncweb.resources.archive.revision;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.exceptions.InvalidRevisionTagException;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XRevisionInfo;

@Path("/archives/{archiveId}/revisions/{revTag}")
public class ArchiveRevisionResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getLatest(
			@PathParam("archiveId") String archiveId,
			@PathParam("revTag") String revTag) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		try(ZKFS fs = State.sharedState().fsForRevision(config, revTag)) {
			RevisionTag tag = fs.getBaseRevision();
			XRevisionInfo xInfo = new XRevisionInfo(tag);
			return XAPIResponse.withPayload(xInfo);
		} catch(InvalidRevisionTagException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		}
	}
}
