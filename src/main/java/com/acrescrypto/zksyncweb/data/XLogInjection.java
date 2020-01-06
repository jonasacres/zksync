package com.acrescrypto.zksyncweb.data;

public class XLogInjection {
	private String text;
	private String severity;
	
	public String getText() {
		return text;
	}
	
	public void setText(String text) {
		this.text = text;
	}

	public String getSeverity() {
		return severity;
	}

	public void setSeverity(String severity) {
		this.severity = severity;
	}
}
