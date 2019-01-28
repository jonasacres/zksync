package com.acrescrypto.zksyncweb.data;

import java.util.HashMap;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.acrescrypto.zksyncweb.exceptionmappers.XAPIResponseMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Provider
@JsonSerialize(using = XAPIResponseMapper.class)
public class XAPIResponse extends RuntimeException implements ExceptionMapper<XAPIResponse> {
	private static final long serialVersionUID = 1L;

	private Object resp;
	private Integer status;
	private String errmsg;

	public static XAPIResponse invalidJsonResponse() {
		return withError(400, "Invalid JSON");
	}

	public static XAPIResponse genericServerErrorResponse() {
		return withError(500, "Server error");
	}

	public static XAPIResponse notFoundErrorResponse() {
		return withError(404, "Not found");
	}

	public static XAPIResponse successResponse() {
		return new XAPIResponse(200, null, null);
	}

	public static XAPIResponse withPayload(Object payload) {
		return new XAPIResponse(200, null, payload);
	}

	public static XAPIResponse withWrappedPayload(String fieldName, Object payload) {
		HashMap<String,Object> map = new HashMap<>();
		map.put(fieldName, payload);
		return new XAPIResponse(200, null, map);
	}

	public static XAPIResponse withPayload(int status, Object payload) {
		return new XAPIResponse(status, null, payload);
	}

	public static XAPIResponse withError(Integer status, String message) {
		return new XAPIResponse(status, message, null);
	}

	public static XAPIResponse withError(Integer status, String message, Object payload) {
		return new XAPIResponse(status, message, payload);
	}

	public XAPIResponse() {}

	public XAPIResponse(Integer status, String errmsg, Object payload) {
		this.status = status;
		this.errmsg = errmsg;
		this.resp = payload;
	}

	public Object getResp() {
		return resp;
	}

	public void setResp(Object resp) {
		this.resp = resp;
	}

	public Integer getStatus() {
		return status;
	}

	public void setStatus(Integer status) {
		this.status = status;
	}

	public String getErrmsg() {
		return errmsg;
	}

	public void setErrmsg(String errmsg) {
		this.errmsg = errmsg;
	}

	public Response toResponse() {
		HashMap<String,Object> sanitized = new HashMap<>();
		sanitized.put("status", this.getStatus());
		sanitized.put("errmsg", this.getErrmsg());
		sanitized.put("resp", this.getResp());

		return Response
				.status(this.getStatus())
				.entity(sanitized)
				.type(MediaType.APPLICATION_JSON)
				.build();
	}

	@Override
	public Response toResponse(XAPIResponse exc) {
		return exc.toResponse();
	}
}
