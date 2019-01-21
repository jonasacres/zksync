package com.acrescrypto.zksyncweb.serializers;

import java.io.IOException;

import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

public class RevisionTagSerializer extends StdSerializer<RevisionTag> {
	
	public RevisionTagSerializer() {
		this(null);
	}

	protected RevisionTagSerializer(Class<RevisionTag> t) {
		super(t);
	}

	@Override
	public void serialize(RevisionTag value, JsonGenerator jgen, SerializerProvider provider)
			throws IOException, JsonGenerationException {
		jgen.writeBinary(value.getBytes());
	}
}
