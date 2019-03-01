package com.acrescrypto.zksyncweb.resources;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;

import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.fs.FS;
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
		try {
			throw XAPIResponse.withPayload(State.sharedState().getMaster().getGlobalConfig().asHash());
		} catch(XAPIResponse exc) {
			throw exc;
		} catch(Exception exc) {
			exc.printStackTrace();
			throw exc;
		}
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
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/filehandles")
	public XAPIResponse getFileHandles() throws IOException {
		LinkedList<HashMap<String,Object>> files = new LinkedList<>();
		FS.getGlobalOpenFiles().forEach((file, trace) -> {
			HashMap<String, Object> info = new HashMap<>();
			LinkedList<HashMap<String,Object>> traceListing = new LinkedList<>();
			info.put("path", file.getPath());
			info.put("fsClass", file.getFs().getClass().getCanonicalName());
			for(StackTraceElement element : trace.getStackTrace()) {
				HashMap<String,Object> frame = new HashMap<>();
				frame.put("file", element.getFileName());
				frame.put("method", element.getMethodName());
				frame.put("line", element.getLineNumber());
				traceListing.add(frame);
			}
			info.put("trace", traceListing);
			files.add(info);
		});
		throw XAPIResponse.withWrappedPayload("files", files);
	}
}
