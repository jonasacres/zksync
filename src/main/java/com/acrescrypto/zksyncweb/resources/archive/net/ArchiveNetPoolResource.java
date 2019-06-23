package com.acrescrypto.zksyncweb.resources.archive.net;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XNetPoolStats;

@Path("/archives/{archiveid}/net/pool")
public class ArchiveNetPoolResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getPool(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		throw XAPIResponse.withPayload(new XNetPoolStats(config.getSwarm().getRequestPool()));
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("pages")
	public XAPIResponse getPages(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		throw XAPIResponse.withWrappedPayload("pages", config.getSwarm().getRequestPool().requestedPageTags());
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("inodes")
	public XAPIResponse getInodes(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		throw XAPIResponse.withWrappedPayload("inodes", config.getSwarm().getRequestPool().requestedInodes());
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("revisions")
	public XAPIResponse getRevisions(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		throw XAPIResponse.withWrappedPayload("revisions", config.getSwarm().getRequestPool().requestedRevisions());
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("revisionstructures")
	public XAPIResponse getRevisionStructures(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		throw XAPIResponse.withWrappedPayload("revisionStructures", config.getSwarm().getRequestPool().requestedRevisionStructures());
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("revisiondetails")
	public XAPIResponse getRevisionDetails(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		throw XAPIResponse.withWrappedPayload("revisionDetails", config.getSwarm().getRequestPool().requestedRevisionDetails());
	}
}
