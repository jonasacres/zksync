package com.acrescrypto.zksyncweb.exceptionmappers;

import java.io.IOException;

import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

@Provider
public class XAPIResponseMapper extends JsonSerializer<XAPIResponse> implements ExceptionMapper<XAPIResponse> {
    @Override
    public Response toResponse(XAPIResponse exc) {
    	return exc.toResponse();
    }

	@Override
	public void serialize(XAPIResponse value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonProcessingException {
		jgen.writeStartObject();
		jgen.writeNumberField("status", value.getStatus());
		jgen.writeStringField("errmsg", value.getErrmsg());
		jgen.writeObjectField("resp", value.getResp());
		jgen.writeEndObject();
	}
}
