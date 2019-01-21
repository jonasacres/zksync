package com.acrescrypto.zksyncweb.resources.blacklist;

import java.io.IOException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.BlacklistEntry;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;

@Path("/blacklist/{ip}")
public class BlacklistEntryResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getBlacklistEntry(@PathParam("ip") String ip) throws IOException {
		Blacklist blacklist = State.sharedState().getMaster().getBlacklist();
		BlacklistEntry entry = blacklist.get(ip);
		if(entry == null) throw XAPIResponse.notFoundErrorResponse();
		
		return XAPIResponse.withPayload(entry);
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse deleteBlacklistEntry(@PathParam("ip") String ip) throws IOException {
		Blacklist blacklist = State.sharedState().getMaster().getBlacklist();
		BlacklistEntry entry = blacklist.get(ip);
		if(entry == null) throw XAPIResponse.notFoundErrorResponse();
		
		blacklist.remove(ip);
		return XAPIResponse.successResponse();
	}
}
