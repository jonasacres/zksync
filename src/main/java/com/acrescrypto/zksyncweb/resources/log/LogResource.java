package com.acrescrypto.zksyncweb.resources.log;

import java.io.IOException;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.MemLogAppender;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksync.utility.MemLogAppender.LogEvent;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XLogInfo;
import com.acrescrypto.zksyncweb.data.XLogInjection;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import ch.qos.logback.classic.Level;

@Path("/logs")
public class LogResource {
	private Logger logger = LoggerFactory.getLogger(LogResource.class);
			
	public static int intForLevelName(String name) {
		return Level.toLevel(name.toUpperCase()).toInt();
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.TEXT_PLAIN)
	public void getLogs(
			@DefaultValue("-1") @QueryParam("offset") int offset,
			@DefaultValue("1000") @QueryParam("length") int length,
			@DefaultValue("info") @QueryParam("level") String thresholdName,
			@DefaultValue("-1") @QueryParam("launchTime") long launchTime,
			@DefaultValue("-1") @QueryParam("after") long after,
			@DefaultValue("9223372036854775807") @QueryParam("before") long before,
			@Context UriInfo uriInfo
			) {
		int threshold = intForLevelName(thresholdName);
		if(launchTime >= 0 && launchTime != Util.launchTime()) {
			offset = -1;
			after = -1;
			before = Long.MAX_VALUE;
		}
		
		List<LogEvent> entries = MemLogAppender
				.sharedInstance()
				.getEntries(offset, length, threshold, after, before);
		
		throw XAPIResponse.withPayload(new XLogInfo(entries));
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public void postLogs(String json) throws JsonParseException, JsonMappingException, IOException {
		XLogInjection injection = new ObjectMapper().readValue(json, XLogInjection.class);
		
		String level = injection.getSeverity();
		if(level == null) level = "info";
		else level = level.toLowerCase();
		
		switch(level) {
		case "trace":
			logger.trace(injection.getText());
			break;
		case "debug":
			logger.debug(injection.getText());
			break;
		case "info":
			logger.info(injection.getText());
			break;
		case "warn":
			logger.warn(injection.getText());
			break;
		case "error":
			logger.error(injection.getText());
			break;
		default:
			logger.info(injection.getText());
		}
		
		throw XAPIResponse.successResponse();
	}
}
