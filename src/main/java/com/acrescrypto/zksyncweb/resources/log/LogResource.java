package com.acrescrypto.zksyncweb.resources.log;

import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.acrescrypto.zksync.utility.MemLogAppender;
import com.acrescrypto.zksync.utility.MemLogAppender.LogEvent;
import com.acrescrypto.zksyncweb.data.XAPIResponse;

import ch.qos.logback.classic.Level;

@Path("/logs")
public class LogResource {
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
			@DefaultValue("-1") @QueryParam("after") long after,
			@DefaultValue("9223372036854775807") @QueryParam("before") long before,
			@Context UriInfo uriInfo
			) {
		int threshold = intForLevelName(thresholdName);
		List<LogEvent> entries = MemLogAppender
				.sharedInstance()
				.getEntries(offset, length, threshold, after, before);
		
		throw XAPIResponse.withWrappedPayload("entries", entries);
	}
}
