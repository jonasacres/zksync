package com.acrescrypto.zksyncweb.resources.archive.revision;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.OldRevisionTree;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.ArchiveCrud;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XRevisionInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/archives/{archiveId}/revisions")
public class ArchiveRevisionsResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getRevisions(
			@PathParam("archiveId") String archiveId,
			@DefaultValue("tips") @QueryParam("mode") String mode,
			@Context UriInfo uriInfo
			) throws XAPIResponse, IOException {
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		int depth = Integer.parseInt(params.getOrDefault("depth", "1"));
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		if(mode.equals("all")) {
			return listAll(config, depth);
		}
		
		ArrayList<XRevisionInfo> tips = new ArrayList<>();
		for(RevisionTag tip :  config.getRevisionList().branchTips()) {
			tips.add(new XRevisionInfo(tip, depth));
		}
		
		return XAPIResponse.withWrappedPayload("branchTips", tips);
	}
	
	protected XAPIResponse listAll(ZKArchiveConfig config, int depth) throws IOException {
		HashSet<XRevisionInfo> set = new HashSet<>();
		LinkedList<RevisionTag> queue = new LinkedList<>();

		AsyncRevisionTree tree = config.getRevisionTree();
		
		queue.addAll(config.getRevisionList().branchTips());
		while(!queue.isEmpty()) {
			RevisionTag tip = queue.pop();
			queue.addAll(tree.parentsForTag(tip));
			set.add(new XRevisionInfo(tip, depth));
		}
		
		return XAPIResponse.withWrappedPayload("revisions", set);
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
			info.setParents(new XRevisionInfo[0]);
		}
		
		if(config.getAccessor().isSeedOnly() || config.isReadOnly()) {
			throw XAPIResponse.withError(400, "Archive not writable");
		}
		
		RevisionTag[] parents = new RevisionTag[info.getParents().length];
		for(int i = 0; i < info.getParents().length; i++) {
			try {
				parents[i] = new RevisionTag(config, info.getParents()[i].getRevTag(), true);
			} catch(Throwable exc) {
				throw XAPIResponse.withError(400, "Invalid parent tag");
			}
		}
		
		RevisionTag tag = State.sharedState().activeFs(config).commit(parents);
		return XAPIResponse.withWrappedPayload("revTag", Base64.getEncoder().encodeToString(tag.getBytes()));
	}
}
