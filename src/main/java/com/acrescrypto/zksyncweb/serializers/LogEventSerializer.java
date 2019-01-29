package com.acrescrypto.zksyncweb.serializers;

import java.io.IOException;

import com.acrescrypto.zksync.utility.MemLogAppender.LogEvent;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

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
		jgen.writeStartObject();
		jgen.writeNumberField("id", value.getEntryId());
		jgen.writeNumberField("time", value.getEntry().getTimeStamp());
		jgen.writeNumberField("level", value.getEntry().getLevel().toInt());
		jgen.writeStringField("thread", value.getEntry().getThreadName());
		jgen.writeStringField("logger", value.getEntry().getLoggerName());
		jgen.writeStringField("msg", value.getEntry().getFormattedMessage());
		jgen.writeEndObject();
	}
}
