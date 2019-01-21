package com.acrescrypto.zksyncweb.serializers;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class ILoggingEventSerializer extends StdSerializer<ILoggingEvent> {
	
	public ILoggingEventSerializer() {
		this(null);
	}

	protected ILoggingEventSerializer(Class<ILoggingEvent> t) {
		super(t);
	}

	@Override
	public void serialize(ILoggingEvent value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonGenerationException {
		jgen.writeStartObject();
		jgen.writeNumberField("time", value.getTimeStamp());
		jgen.writeNumberField("level", value.getLevel().toInt());
		jgen.writeStringField("thread", value.getThreadName());
		jgen.writeStringField("logger", value.getLoggerName());
		jgen.writeStringField("msg", value.getFormattedMessage());
		jgen.writeEndObject();
	}
}
