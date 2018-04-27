package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;

public abstract class PeerSocket {
	protected PeerSwarm swarm;
	protected LinkedList<PeerMessage> outgoing = new LinkedList<PeerMessage>();
	protected LinkedList<PeerMessage> ready = new LinkedList<PeerMessage>();
	
	protected HashMap<Integer,PeerMessage> waitingResponse = new HashMap<Integer,PeerMessage>();
	protected HashMap<Integer,PeerMessage> incomingRequests = new HashMap<Integer,PeerMessage>();
	
	protected int nextMessageId;
	
	private String address;
	
	public final static String EXT_FULL_PEER = "EXT_FULL_PEER";
	
	public static boolean addressSupported(String address) {
		return false;
	}
	
	public static PeerSocket connectToAddress(PeerSwarm swarm, String address) throws UnsupportedProtocolException {
		if(!addressSupported(address)) throw new UnsupportedProtocolException();
		// TODO P2P: decode URL, select address
		return null;
	}
	
	public interface PeerSocketDelegate {
		public void receivedMessage(PeerMessage message) throws ProtocolViolationException;
		public void establishedSalt(byte[] sharedSalt);
	}
	
	public boolean hasExtension(String extName) {
		return false;
	}
	
	public Object extensionValue(String extName) {
		return null;
	}
	
	public abstract void write(byte[] data, int offset, int length);
	public abstract void read(byte[] data, int offset, int length);
	public abstract boolean isClient();
	public abstract void close();
	
	public abstract byte[] getSharedSecret();
	
	public PeerSwarm getSwarm() {
		return swarm;
	}

	public String getAddress() {
		return address;
	}

	public int maxPayloadSize() {
		return 64*1024;
	}
	
	/** Immediately close socket and blacklist due to a clear protocol violation. */
	public void violation() {
		// TODO P2P: close and blacklist
	}
	
	/** Close connection and add strikes against an address; blacklist triggered when strikes reach threshold. */
	public void violation(int strikes) {
		close();
	}
	
	/** Handle some sort of I/O exception */
	public void ioexception(IOException exc) {
		/* These are probably our fault. There is the possibility that they are caused by malicious actors. */
		close();
	}
	
	protected void sendMessage(PeerMessage msg) throws IOException {
		write(msg.txBuf.array(), 0, msg.txBuf.position());
		msg.clearTxBuf();
		outgoing.remove(msg);
		if(!msg.txClosed()) {
			outgoing.add(msg);
		}
	}
	
	protected void sendThread() {
		new Thread(() -> {
			try {
				synchronized(this) {
					while(!ready.isEmpty()) {
						sendMessage(ready.remove());
					}
				}
				
				try {
					outgoing.wait();
				} catch (InterruptedException e) {}
			} catch (IOException exc) {
				ioexception(exc);
			}
		}).start();
	}
	
	protected void recvThread() {
		new Thread(() -> {
			try {
				while(true) {
					ByteBuffer buf = ByteBuffer.allocate(maxPayloadSize() + PeerMessage.headerLength());
					read(buf.array(), 0, PeerMessage.headerLength());
					
					int msgId = buf.getInt();
					int len = buf.getInt();
					byte cmd = buf.get(), flags = buf.get();
					assertState(len <= maxPayloadSize());
					read(buf.array(), buf.position(), len);
					
					if((msgId & 0x7FFFFFFF) == 0) {
						incomingRequests.putIfAbsent(msgId, new PeerMessage(this, cmd, msgId));				
						incomingRequests.get(msgId).addPayload(buf.array());
						if((flags & PeerMessage.FLAG_FINAL) != 0) incomingRequests.remove(msgId);
					} else {
						msgId &= 0x7FFFFFFF;
						assertState(waitingResponse.containsKey(msgId));
						waitingResponse.get(msgId).addPayload(buf.array());
						if((flags & PeerMessage.FLAG_FINAL) != 0) waitingResponse.remove(msgId);
					}
				}
			} catch(ProtocolViolationException exc) {
				violation();
			}
		}).start();
	}
	
	protected synchronized void dataReady(PeerMessage msg) {
		ready.add(msg);
		outgoing.notifyAll();
	}
	
	public synchronized int issueMessageId() {
		return nextMessageId++;
	}
	
	protected void assertState(boolean test) throws ProtocolViolationException {
		if(!test) throw new ProtocolViolationException();
	}
}
