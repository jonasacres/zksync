package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;

public abstract class PeerSocket {
	protected PeerSwarm swarm;
	protected PeerSocketDelegate delegate;
	protected LinkedList<PeerMessageOutgoing> outgoing = new LinkedList<PeerMessageOutgoing>();
	protected LinkedList<PeerMessageOutgoing> ready = new LinkedList<PeerMessageOutgoing>();	
	protected HashMap<Integer,PeerMessageIncoming> incoming = new HashMap<Integer,PeerMessageIncoming>();
	
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
		public void handle(PeerMessageIncoming message) throws ProtocolViolationException, EOFException;
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
	
	protected void sendMessage(PeerMessageOutgoing msg) throws IOException {
		write(msg.txBuf.array(), 0, msg.txBuf.position());
		msg.clearTxBuf();
		if(msg.txClosed()) {
			outgoing.remove(msg);
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
					ByteBuffer buf = ByteBuffer.allocate(maxPayloadSize() + PeerMessage.HEADER_LENGTH);
					read(buf.array(), 0, PeerMessageIncoming.HEADER_LENGTH);
					
					int msgId = buf.getInt();
					int len = buf.getInt();
					byte cmd = buf.get(), flags = buf.get();
					assertState(len <= maxPayloadSize());
					
					byte[] payload = new byte[len];
					read(payload, 0, payload.length);
					
					if(!incoming.containsKey(msgId)) {
						// TODO P2P: this is a conundrum... replace delegate with connection?
						incoming.put(msgId, new PeerMessageIncoming(connection, cmd, flags, msgId));
					}
					
					PeerMessageIncoming msg = incoming.get(msgId);
					msg.receivedData(flags, payload);
					if(msg.rxBuf.isEOF()) {
						incoming.remove(msgId);
					}
				}
			} catch(ProtocolViolationException exc) {
				violation();
			}
		}).start();
	}
	
	protected synchronized void dataReady(PeerMessageOutgoing msg) {
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
