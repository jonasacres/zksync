package com.acrescrypto.zksync.net;

public interface PeerNetSocket {
	public interface PeerNetSocketListener {
		public void receive(byte[] message);
	}
	
	public String getAddress();
	public void send(byte[] bytes, PeerNetSocketListener responseHandler);
	public void setListener(PeerNetSocketListener listener);
	public void close();
	public void graylist();
	public void blacklist();
	public void enqueue(RemoteRequest req);
}
