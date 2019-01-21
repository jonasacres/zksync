package com.acrescrypto.zksyncweb.data;

import java.util.HashMap;
import java.util.Map;

public class XSoftwareStack {
	private Map<String, XSoftwareVersion> versions;
	
	public XSoftwareStack() {
		this.versions = new HashMap<>();
	}
	
	public void addVersion(String name, XSoftwareVersion version) {
		this.versions.put(name, version);
	}
	
	public void setVersions(Map<String, XSoftwareVersion> versions) {
		this.versions = versions;
	}
	
	public Map<String, XSoftwareVersion> getVersions() {
		return versions;
	}
}
