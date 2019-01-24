package com.acrescrypto.zksyncweb.exceptionmappers;

import javax.servlet.http.HttpServletRequest;
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

	@Context private HttpHeaders headers;
	@Context private HttpServletRequest request;

	@Override
	public Response toResponse(Throwable exception) {
		if(exception instanceof NotFoundException) {
			return XAPIResponse.withError(404, "Not found").toResponse();
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
