package com.acrescrypto.zksyncweb.serializers;

import java.io.IOException;

import com.acrescrypto.zksync.utility.MemLogAppender.LogEvent;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.StackTraceElementProxy;

public class LogEventSerializer extends StdSerializer<LogEvent> {
	
	public LogEventSerializer() {
		this(null);
	}

	protected LogEventSerializer(Class<LogEvent> t) {
		super(t);
	}

	@Override
	public void serialize(LogEvent value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonGenerationException {
		try {
			System.out.println(value.getEntry().getThrowableProxy().getMessage());
		} catch(Exception exc) {}
		jgen.writeStartObject();
		jgen.writeNumberField("id", value.getEntryId());
		jgen.writeNumberField("time", value.getEntry().getTimeStamp());
		jgen.writeNumberField("level", value.getEntry().getLevel().toInt());
		jgen.writeStringField("thread", value.getEntry().getThreadName());
		jgen.writeStringField("logger", value.getEntry().getLoggerName());
		jgen.writeStringField("msg", value.getEntry().getFormattedMessage());
		
		IThrowableProxy proxy = value.getEntry().getThrowableProxy();
		if(proxy != null) {
			jgen.writeObjectFieldStart("exception");
			jgen.writeStringField("class", proxy.getClassName());
			jgen.writeStringField("msg", proxy.getMessage());
			StackTraceElementProxy[] stack = proxy.getStackTraceElementProxyArray();
			if(stack != null) {
				jgen.writeArrayFieldStart("stack");
				for(StackTraceElementProxy entry : stack) {
					jgen.writeStartObject();
					jgen.writeStringField("file", entry.getStackTraceElement().getFileName());
					jgen.writeStringField("method", entry.getStackTraceElement().getMethodName());
					jgen.writeNumberField("line", entry.getStackTraceElement().getLineNumber());
					jgen.writeEndObject();
				}
				jgen.writeEndArray();
			} else {
				jgen.writeNullField("stack");
			}
			jgen.writeEndObject();
		} else {
			jgen.writeNullField("exception");
		}
		
		jgen.writeEndObject();
	}
}
