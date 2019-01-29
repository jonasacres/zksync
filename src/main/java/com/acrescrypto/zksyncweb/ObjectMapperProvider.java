package com.acrescrypto.zksyncweb;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.utility.MemLogAppender.LogEvent;
import com.acrescrypto.zksyncweb.serializers.LogEventSerializer;
import com.acrescrypto.zksyncweb.serializers.RevisionTagSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;

@Provider
public class ObjectMapperProvider implements ContextResolver<ObjectMapper> {

	final ObjectMapper defaultObjectMapper;

	public ObjectMapperProvider() {
		defaultObjectMapper = createDefaultMapper();
	}

	@Override
	public ObjectMapper getContext(Class<?> type) {
		return defaultObjectMapper;
	}

	private static ObjectMapper createDefaultMapper() {
		final ObjectMapper mapper = new ObjectMapper();

		SimpleModule module = new SimpleModule();
		module.addSerializer(RevisionTag.class, new RevisionTagSerializer());
		module.addSerializer(LogEvent.class, new LogEventSerializer());
		mapper.registerModule(module);

		// uncomment below to indent output, nice for debugging
		mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

		return mapper;
	}
}
