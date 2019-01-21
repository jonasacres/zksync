package com.acrescrypto.zksyncweb.resources;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XSoftwareStack;
import com.acrescrypto.zksyncweb.data.XSoftwareVersion;
import com.acrescrypto.zksync.fs.zkfs.ZKVersion;
import com.acrescrypto.zksyncweb.VersionInfo;

@Path("version")
public class VersionResource {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public XAPIResponse getVersion() {
    	XSoftwareStack stack = new XSoftwareStack();
    	stack.addVersion("api", new XSoftwareVersion(VersionInfo.NAME, VersionInfo.MAJOR, VersionInfo.MINOR, VersionInfo.REVISION));
    	stack.addVersion("fs", new XSoftwareVersion(ZKVersion.NAME, ZKVersion.MAJOR, ZKVersion.MINOR, ZKVersion.REVISION));
    	return XAPIResponse.withPayload(stack);
    }
}
