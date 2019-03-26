package com.acrescrypto.zksyncweb.resources.archive.revision;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksyncweb.ArchiveCrud;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;

@Path("/archives/{archiveId}/revisions/{revTag}/fs{path:.*}")
public class ArchiveRevisionFsResource {
	@GET
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public byte[] getPath(
			@PathParam("archiveId") String archiveId,
			@PathParam("revTag") String revTag,
			@PathParam("path") String fullPath,
			@Context UriInfo uriInfo) throws XAPIResponse, IOException {
		String path = ArchiveCrud.basePath(fullPath);
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		try(ZKFS fs = State.sharedState().fsForRevision(config, revTag)) {
			return ArchiveCrud.get(fs, path, params);
		}
	}
}
