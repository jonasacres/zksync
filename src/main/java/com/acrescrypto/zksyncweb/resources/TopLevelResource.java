package com.acrescrypto.zksyncweb.resources;

import java.io.IOException;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;

@Path("/")
public class TopLevelResource {
	protected Logger logger = LoggerFactory.getLogger(GlobalResource.class);
	
	@GET
	public XAPIResponse get() throws IOException {
		throw XAPIResponse.successResponse();
	}
	
	@DELETE
	public XAPIResponse deleteEverything() throws IOException {
		logger.warn("Purging storage from api call 'DELETE /'");
		
		FS storage = State.sharedState().getMaster().getStorage(); 
		State.resetState(()->{
			try {
				storage.purge();
			} catch (IOException exc) {
				logger.error("Caught exception purging storage", exc);
				throw XAPIResponse.withError(500, "Encountered exception purging storage");
			}
		});
		
		throw XAPIResponse.successResponse();
	}
}
