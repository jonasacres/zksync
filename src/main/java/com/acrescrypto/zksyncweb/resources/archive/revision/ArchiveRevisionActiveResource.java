package com.acrescrypto.zksyncweb.resources.archive.revision;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.acrescrypto.zksync.exceptions.InvalidRevisionTagException;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksyncweb.ArchiveCrud;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XRevisionInfo;
import com.acrescrypto.zksyncweb.data.XRevisionPrefix;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/archives/{archiveId}/revisions/active")
public class ArchiveRevisionActiveResource {
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getActive(@PathParam("archiveId") String archiveId,
			@Context UriInfo uriInfo) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		int depth = Integer.parseInt(params.getOrDefault("depth", "1"));
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		XRevisionInfo xInfo = new XRevisionInfo(State.sharedState().activeFs(config).getBaseRevision(), depth);
		return XAPIResponse.withPayload(xInfo);
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse putActive(@PathParam("archiveId") String archiveId, String json) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		XRevisionPrefix info = new ObjectMapper().readValue(json, XRevisionPrefix.class);
		
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		try(ZKFS fs = State.sharedState().fsForRevision(config, info.getRevTag())) {
			State.sharedState().setActiveFs(config, fs);
			return XAPIResponse.withPayload(new XRevisionInfo(fs.getBaseRevision(), 1));
		} catch(InvalidRevisionTagException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		}
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse deleteActive(@PathParam("archiveId") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		RevisionTag latest = config.getRevisionList().latest();
		try(ZKFS fs = latest.getFS()) {
			State.sharedState().activeManager(config).setFs(fs);
		}
		
		return XAPIResponse
				.withPayload(new XRevisionInfo(State.sharedState().activeFs(config).getBaseRevision(), 1));
	}	
}
