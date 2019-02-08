package com.acrescrypto.zksyncweb;

import java.io.IOException;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Provider
public class CustomLoggingFilter implements ContainerRequestFilter, ContainerResponseFilter {
	@Context
	private ResourceInfo resourceInfo;
	
	private	Logger logger = LoggerFactory.getLogger(CustomLoggingFilter.class);

	@Override
	public void filter(ContainerRequestContext requestContext) throws IOException {
		boolean includeLog = State.sharedState().getMaster().getGlobalConfig().getBool("log.includeLogRequests");
		if(!includeLog
				&& requestContext.getMethod().equals("GET")
				&& requestContext.getUriInfo().getAbsolutePath().equals("/logs")) {
			return;
		}
		logger.debug("{} {} -- {} bytes",
				requestContext.getMethod(),
				requestContext.getUriInfo().getRequestUri(),
				requestContext.getLength());
	}

	@Override
	public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
			throws IOException {
	}
}
