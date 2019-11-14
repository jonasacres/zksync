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
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFile;
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
			} else if(v.isNull()) {
				throw XAPIResponse.withError(400, "Cannot specify null values");
			}
		});
		
		throw XAPIResponse.successResponse();
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/uptime")
	public XAPIResponse getUptime() throws IOException {
		HashMap<String,Object> result = new HashMap<>();
		result.put("uptime", System.currentTimeMillis() - Util.launchTime());
		result.put("launchTimeMs", Util.launchTime());
		
		throw XAPIResponse.withPayload(200, result);
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/filehandles")
	public XAPIResponse getFileHandles() throws IOException {
		LinkedList<HashMap<String,Object>> files = new LinkedList<>();
		FS.getGlobalOpenFiles().forEach((file, trace) -> {
			HashMap<String, Object> info = new HashMap<>();
			info.put("trace", renderStackTrace(trace));
			info.put("path", file.getPath());
			if(file instanceof ZKFile) {
				ZKFile zkfile = (ZKFile) file;
				info.put("retainCount", zkfile.getRetainCount());
			}
			info.put("fsClass", file.getFs().getClass().getCanonicalName());
			files.add(info);
		});
		throw XAPIResponse.withWrappedPayload("files", files);
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/archives")
	public XAPIResponse getArchives() throws IOException {
		LinkedList<HashMap<String,Object>> archives = new LinkedList<>();
		ZKArchive.getActiveArchives().forEach((archive, trace)->{
			HashMap<String, Object> info = new HashMap<>();
			info.put("archiveId", archive.getConfig().getArchiveId());
			info.put("trace", renderStackTrace(trace));
			archives.add(info);
		});
		throw XAPIResponse.withWrappedPayload("archives", archives);
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/filesystems")
	public XAPIResponse getFilesystems() throws IOException {
		LinkedList<HashMap<String,Object>> filesystems = new LinkedList<>();
		ZKFS.getOpenInstances().forEach((fs, trace) -> {
			HashMap<String, Object> info = new HashMap<>();
			info.put("fsClass", fs.getClass().getCanonicalName());
			info.put("trace", renderStackTrace(trace));
			info.put("directoryCacheSize", fs.getDirectoryCacheSize());
			
			LinkedList<LinkedList<HashMap<String, Object>>> retentions = new LinkedList<>();
			for(Throwable retention : fs.getRetentions()) {
				retentions.add(renderStackTrace(retention));
			}
			info.put("retentions", retentions);
			
			LinkedList<LinkedList<HashMap<String, Object>>> closures = new LinkedList<>();
			for(Throwable closure : fs.getClosures()) {
				closures.add(renderStackTrace(closure));
			}
			info.put("closures", closures);
			
			filesystems.add(info);
		});
		throw XAPIResponse.withWrappedPayload("filesystems", filesystems);
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/threads")
	public XAPIResponse getThreads() throws IOException {
		throw XAPIResponse.withWrappedPayload("report", Util.threadReport());
	}
	
	public LinkedList<HashMap<String,Object>> renderStackTrace(Throwable trace) {
		LinkedList<HashMap<String,Object>> traceListing = new LinkedList<>();
		for(StackTraceElement element : trace.getStackTrace()) {
			HashMap<String,Object> frame = new HashMap<>();
			frame.put("file", element.getFileName());
			frame.put("method", element.getMethodName());
			frame.put("line", element.getLineNumber());
			traceListing.add(frame);
		}
		
		return traceListing;
	}
}
