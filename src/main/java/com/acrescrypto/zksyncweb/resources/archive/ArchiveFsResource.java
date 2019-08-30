package com.acrescrypto.zksyncweb.resources.archive;

import java.io.IOException;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
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

@Path("/archives/{archiveId}/fs{path:.*}")
public class ArchiveFsResource {
	@GET
	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	public byte[] getPath(
			@PathParam("archiveId") String archiveId,
			@PathParam("path") String fullPath,
			@Context UriInfo uriInfo
			) throws XAPIResponse, IOException {
		String path = ArchiveCrud.basePath(fullPath);
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		ZKFS fs = State.sharedState().activeFs(config);
		return ArchiveCrud.get(fs, path, params);
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse postPath(
			@PathParam("archiveId") String archiveId,
			@PathParam("path") String fullPath,
			@Context UriInfo uriInfo,
			byte[] contents) throws XAPIResponse, IOException {
		String path = ArchiveCrud.basePath(fullPath);
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		ZKFS fs = State.sharedState().activeFs(config);

		return ArchiveCrud.post(fs, path, params, contents);
	}
	
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse putPath(
			@PathParam("archiveId") String archiveId,
			@PathParam("path") String fullPath,
			@Context UriInfo uriInfo,
			byte[] contents) throws XAPIResponse, IOException {
		String path = ArchiveCrud.basePath(fullPath);
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		ZKFS fs = State.sharedState().activeFs(config);

		return ArchiveCrud.put(fs, path, params, contents);
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse deletePath(
			@PathParam("archiveId") String archiveId,
			@PathParam("path") String fullPath,
			@Context UriInfo uriInfo) throws XAPIResponse, IOException {
		String path = ArchiveCrud.basePath(fullPath);
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();		
		ZKFS fs = State.sharedState().activeFs(config);
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		
		return ArchiveCrud.delete(fs, path, params);
	}
}
