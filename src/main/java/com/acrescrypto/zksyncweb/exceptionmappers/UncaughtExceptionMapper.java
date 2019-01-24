package com.acrescrypto.zksyncweb.exceptionmappers;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksyncweb.data.XAPIResponse;

@Provider
public class UncaughtExceptionMapper implements ExceptionMapper<Throwable> {
	private	Logger logger = LoggerFactory.getLogger(UncaughtExceptionMapper.class);

	@Context
	private HttpHeaders headers;

	@Override
	public Response toResponse(Throwable exception) {
		if(exception instanceof NotFoundException) {
			return XAPIResponse.withError(404, "Not found").toResponse();
		}
		
		logger.warn("Caught exception: {}",
				exception);
		return XAPIResponse.withError(500, "Server error").toResponse();
	}
}
