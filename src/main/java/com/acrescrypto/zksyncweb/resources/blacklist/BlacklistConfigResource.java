package com.acrescrypto.zksyncweb.resources.blacklist;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.net.Blacklist;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.WebUtils;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XBlacklistConfig;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/blacklist/config")
public class BlacklistConfigResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getBlacklistConfig() throws IOException {
		return XAPIResponse.withPayload(new XBlacklistConfig(State.sharedState().getMaster().getBlacklist()));
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse putBlacklistConfig(String json) throws IOException {
		XBlacklistConfig config = new ObjectMapper().readValue(json, XBlacklistConfig.class);
		Blacklist blacklist = State.sharedState().getMaster().getBlacklist();
		WebUtils.mapFieldWithException(config.isEnabled(), (enabled)->blacklist.setEnabled(enabled));
		return XAPIResponse.successResponse();
	}
}
