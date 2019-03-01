package com.acrescrypto.zksyncweb.serializers;

import java.io.IOException;

import javax.ws.rs.ext.Provider;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

@Provider
public class ThrowableSerializer extends StdSerializer<Throwable> {
	public ThrowableSerializer() {
		this(null);
	}
	
	protected ThrowableSerializer(Class<Throwable> t) {
		super(t);
	}
	
	@Override
	public void serialize(Throwable value, JsonGenerator jgen, SerializerProvider provider) throws JsonGenerationException, IOException {
		jgen.writeStartObject();
		jgen.writeStringField("class", value.getClass().getCanonicalName());
		jgen.writeStringField("msg", value.getMessage());
		
		StackTraceElement[] stack = value.getStackTrace();
		
		if(stack != null) {
			jgen.writeArrayFieldStart("stack");
			for(StackTraceElement entry : stack) {
				jgen.writeStartObject();
				jgen.writeStringField("file", entry.getFileName());
				jgen.writeStringField("method", entry.getMethodName());
				jgen.writeNumberField("line", entry.getLineNumber());
				jgen.writeEndObject();
			}
			jgen.writeEndArray();
		} else {
			jgen.writeNullField("stack");
		}
		
		jgen.writeEndObject();
	}
}
