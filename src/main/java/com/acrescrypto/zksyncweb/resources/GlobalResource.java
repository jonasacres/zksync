package com.acrescrypto.zksyncweb.resources;

import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XGlobalInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/global")
public class GlobalResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getGlobal() throws IOException {
		XGlobalInfo info = XGlobalInfo.globalInfo();
		throw XAPIResponse.withPayload(info);
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/settings")
	public XAPIResponse getGlobalSettings() throws IOException {
		throw XAPIResponse.withPayload(State.sharedState().getMaster().getGlobalConfig().asHash());
	}
	
	@PUT
	@Path("/settings")
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse putGlobalSettings(String json) throws IOException {
		JsonNode tree = new ObjectMapper().readTree(json);
		ConfigFile config = State.sharedState().getMaster().getGlobalConfig();
		
		tree.fields().forEachRemaining((field)->{
			JsonNode v = field.getValue();
			if(v.isTextual()) {
				config.set(field.getKey(), v.asText());
			} else if(v.canConvertToInt()) {
				config.set(field.getKey(), v.asInt());
			} else if(v.canConvertToLong()) {
				config.set(field.getKey(), v.asLong());
			} else if(v.isDouble()) {
				config.set(field.getKey(), v.asDouble());
			} else if(v.isBoolean()) {
				config.set(field.getKey(), v.asBoolean());
			}
		});
		
		throw XAPIResponse.successResponse();
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/uptime")
	public XAPIResponse getUptime() throws IOException {
		throw XAPIResponse.withWrappedPayload("uptime", System.currentTimeMillis() - Util.launchTime());
	}
}
