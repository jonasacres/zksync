package com.acrescrypto.zksyncweb.resources.archive.revision;

import java.io.IOException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XRevisionInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/archives/{archiveId}/revisions/active")
public class ArchiveRevisionActiveResource {
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getActive(@PathParam("archiveId") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		XRevisionInfo xInfo = new XRevisionInfo(State.sharedState().activeFs(config).getBaseRevision());
		return XAPIResponse.withPayload(xInfo);
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse putActive(@PathParam("archiveId") String archiveId, String json) throws IOException {
		// technically this just wants an object of the form { "revTag":... }, but we can borrow XRevisionInfo and ignore the unused fields
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		XRevisionInfo info = new ObjectMapper().readValue(json, XRevisionInfo.class);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		RevisionTag revTag = new RevisionTag(config, info.getRevTag(), false);
		State.sharedState().setActiveFs(config, revTag.getFS());
		return XAPIResponse.successResponse();
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse deleteActive(@PathParam("archiveId") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		State.sharedState().activeManager(config).setFs(config.getRevisionList().latest().getFS());
		return XAPIResponse.successResponse();
	}	
}
