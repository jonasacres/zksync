package com.acrescrypto.zksyncweb.resources.archive.revision;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
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

@Path("/archives/{archiveId}/revisions")
public class ArchiveRevisionsResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getPath(@PathParam("archiveId") String archiveId) throws XAPIResponse, IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		ArrayList<byte[]> tips = new ArrayList<>();
		for(RevisionTag tip :  config.getRevisionList().branchTips()) {
			tips.add(tip.getBytes());
		}
		
		return XAPIResponse.withWrappedPayload("branchTips", tips);
	}

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse postCommit(@PathParam("archiveId") String archiveId, String json) throws IOException, XAPIResponse {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		XRevisionInfo info = new XRevisionInfo();
		if(json.length() > 0) {
			info = new ObjectMapper().readValue(json, XRevisionInfo.class);
		} else {
			info.setParents(new byte[0][]);
		}
		
		if(config.getAccessor().isSeedOnly() || config.isReadOnly()) {
			throw XAPIResponse.withError(400, "Archive not writable");
		}
		
		RevisionTag[] parents = new RevisionTag[info.getParents().length];
		for(int i = 0; i < info.getParents().length; i++) {
			try {
				parents[i] = new RevisionTag(config, info.getParents()[i], true);
			} catch(Throwable exc) {
				throw XAPIResponse.withError(400, "Invalid parent tag");
			}
		}
		
		RevisionTag tag = State.sharedState().activeFs(config).commit(parents);
		return XAPIResponse.withWrappedPayload("revTag", Base64.getEncoder().encodeToString(tag.getBytes()));
	}
}
