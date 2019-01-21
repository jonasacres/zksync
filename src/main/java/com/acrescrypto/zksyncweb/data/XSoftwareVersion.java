package com.acrescrypto.zksyncweb.data;

public class XSoftwareVersion {
	private String name;
	private Integer major;
	private Integer minor;
	private Integer revision;
	
	public XSoftwareVersion() {}
	
	public XSoftwareVersion(String name, Integer major, Integer minor, Integer revision) {
		this.name = name;
		this.major = major;
		this.minor = minor;
		this.revision = revision;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public Integer getMajor() {
		return major;
	}
	
	public void setMajor(Integer major) {
		this.major = major;
	}
	
	public Integer getMinor() {
		return minor;
	}
	
	public void setMinor(Integer minor) {
		this.minor = minor;
	}
	
	public Integer getRevision() {
		return revision;
	}
	
	public void setRevision(Integer revision) {
		this.revision = revision;
	}
}
