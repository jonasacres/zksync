package com.acrescrypto.zksyncweb.exceptionmappers;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.NotAllowedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.fasterxml.jackson.core.JsonParseException;

@Provider
public class UncaughtExceptionMapper implements ExceptionMapper<Throwable> {
	private	Logger logger = LoggerFactory.getLogger(UncaughtExceptionMapper.class);

	@Context private HttpHeaders headers;
	@Context private HttpServletRequest request;

	@Override
	public Response toResponse(Throwable exception) {
		if(exception instanceof NotFoundException) {
			return XAPIResponse.withError(404, "Not found").toResponse();
		}
		
		if(exception instanceof NotAllowedException) {
			return XAPIResponse.withError(405, "Method not allowed for this resource").toResponse();
		}
		
		if(exception instanceof JsonParseException) {
			return XAPIResponse.withError(400, "Invalid request JSON").toResponse();
		}
		
		String preamble = "Unknown request";
		if(request != null) {
			String.format("%s %s %s",
					request.getRemoteAddr(),
					request.getMethod(),
					request.getRequestURI());
			
		}
		
		logger.warn("{} caught exception: {} {}",
				preamble,
				exception.getClass().getSimpleName(),
				exception.getMessage(),
				exception);
		return XAPIResponse.withError(500, "Server error").toResponse();
	}
}
