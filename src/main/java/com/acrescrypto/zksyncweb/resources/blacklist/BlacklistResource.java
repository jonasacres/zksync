package com.acrescrypto.zksyncweb.resources.blacklist;

import java.io.IOException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksync.net.BlacklistEntry;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XBlacklistEntry;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/blacklist")
public class BlacklistResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getBlacklist() throws IOException {
		Blacklist blacklist = State.sharedState().getMaster().getBlacklist();
		return XAPIResponse.withWrappedPayload("entries", blacklist.allEntries());
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse postBlacklist(String json) throws IOException {
		BlacklistEntry entry = new ObjectMapper().readValue(json, XBlacklistEntry.class).toBlacklistEntry();
		Blacklist blacklist = State.sharedState().getMaster().getBlacklist();
		
		if(entry.getExpiration() <= System.currentTimeMillis()) {
			throw XAPIResponse.withError(400, "Must specify expiration time in the future");
		}
		
		blacklist.add(entry);
		throw XAPIResponse.withPayload(201, null);
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse deleteBlacklist() throws IOException {
		State.sharedState().getMaster().getBlacklist().clear();
		return XAPIResponse.successResponse();
	}
}
