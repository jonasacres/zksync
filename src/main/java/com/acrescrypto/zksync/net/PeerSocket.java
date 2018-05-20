package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.PeerMessageOutgoing.MessageSegment;

public abstract class PeerSocket {
	protected PeerSwarm swarm;
	protected PeerConnection connection;
	protected LinkedList<PeerMessageOutgoing> outgoing = new LinkedList<PeerMessageOutgoing>();
	protected LinkedList<MessageSegment> ready = new LinkedList<MessageSegment>();	
	protected HashMap<Integer,PeerMessageIncoming> incoming = new HashMap<Integer,PeerMessageIncoming>();
	
	protected int nextMessageId;
	protected final Logger logger = LoggerFactory.getLogger(PeerSocket.class);
	
	private String address;
	
	public final static String EXT_FULL_PEER = "EXT_FULL_PEER";
	
	public static boolean adSupported(PeerAdvertisement ad) {
		return ad.getType() == PeerAdvertisement.TYPE_TCP_PEER;
	}

	public static PeerSocket connectToAd(PeerSwarm swarm, PeerAdvertisement ad) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		if(ad instanceof TCPPeerAdvertisement) {
			return new TCPPeerSocket(swarm, (TCPPeerAdvertisement) ad);
		} else {
			throw new UnsupportedProtocolException();
		}
	}
	
	public boolean hasExtension(String extName) {
		return false;
	}
	
	public Object extensionValue(String extName) {
		return null;
	}
	
	public abstract PeerAdvertisement getAd();

	public abstract void write(byte[] data, int offset, int length) throws IOException, ProtocolViolationException;
	public abstract int read(byte[] data, int offset, int length) throws IOException, ProtocolViolationException;
	public abstract boolean isClient();
	public abstract void close() throws IOException;
	public abstract boolean isClosed();
	
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
	
	/** Immediately close socket and blacklist due to a clear protocol violation. 
	 * @throws IOException */
	public void violation() {
		logger.warn("Logging violation for peer {}", getAddress());
		try {
			close();
			swarm.config.getArchive().getMaster().getBlacklist().add(getAddress(), Integer.MAX_VALUE);
		} catch (IOException exc) {
			logger.warn("Caught exception closing socket to peer {}", getAddress(), exc);
		}
	}
	
	/** Handle some sort of I/O exception */
	public void ioexception(IOException exc) {
		/* These are probably our fault. There is the possibility that they are caused by malicious actors. */
		logger.error("Socket for peer {} caught IOException", getAddress(), exc);
		try {
			close();
		} catch (IOException e) {
			logger.warn("Caught exception closing socket to peer {}", getAddress(), exc);
		}
	}
	
	protected void sendMessage(MessageSegment segment) throws IOException, ProtocolViolationException {
		write(segment.content.array(), 0, segment.content.limit());
		segment.delivered();
		
		if(segment.msg.txClosed()) {
			outgoing.remove(segment.msg);
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
			} catch(Exception exc) {
				logger.error("Socket send thread for peer {} caught exception", getAddress(), exc);
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
					assertState(0 <= len && len <= maxPayloadSize());
					
					byte[] payload = new byte[len];
					read(payload, 0, payload.length);
					
					PeerMessageIncoming msg;
					
					// TODO P2P: (redesign) Stop adversaries from opening a huge number of simultaneous incoming messages
					if(!incoming.containsKey(msgId)) {
						msg = new PeerMessageIncoming(connection, cmd, flags, msgId);
						incoming.put(msgId, msg);
					} else {
						msg = incoming.get(msgId);
					}
					
					msg.receivedData(flags, payload);
					if(msg.rxBuf.isEOF()) {
						incoming.remove(msgId);
					}
				}
			} catch(ProtocolViolationException exc) {
				violation();
			} catch(Exception exc) {
				logger.error("Socket receive thread for {} caught exception", getAddress(), exc);
				violation();
			}
		}).start();
	}
	
	protected synchronized void dataReady(MessageSegment segment) {
		ready.add(segment);
		outgoing.notifyAll();
	}
	
	public synchronized int issueMessageId() {
		return nextMessageId++;
	}
	
	protected void assertState(boolean test) throws ProtocolViolationException {
		if(!test) throw new ProtocolViolationException();
	}

	public boolean matchesAddress(String address) {
		return getAddress().equals(address);
	}
}
