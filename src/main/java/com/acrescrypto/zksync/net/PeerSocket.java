package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.io.InputStream;
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
	
	protected int nextSendMessageId, maxReceivedMessageId;
	protected final Logger logger = LoggerFactory.getLogger(PeerSocket.class);
	
	protected String address;
	
	public final static String EXT_FULL_PEER = "EXT_FULL_PEER";

	public abstract PeerAdvertisement getAd();

	public abstract void write(byte[] data, int offset, int length) throws IOException, ProtocolViolationException;
	public abstract int read(byte[] data, int offset, int length) throws IOException, ProtocolViolationException;
	public abstract boolean isLocalRoleClient();
	public abstract void close() throws IOException;
	public abstract boolean isClosed();
	public abstract void handshake() throws ProtocolViolationException, IOException;
	public abstract int getPeerType() throws UnsupportedOperationException;
	
	public abstract byte[] getSharedSecret();
	
	
	public static boolean adSupported(PeerAdvertisement ad) {
		return ad.getType() == PeerAdvertisement.TYPE_TCP_PEER;
	}

	public static PeerSocket connectToAd(PeerSwarm swarm, PeerAdvertisement ad) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		switch(ad.getType()) {
		case PeerAdvertisement.TYPE_TCP_PEER:
			return new TCPPeerSocket(swarm, (TCPPeerAdvertisement) ad);
		default:
			throw new UnsupportedProtocolException();
		}
	}
	
	public boolean hasExtension(String extName) {
		return false;
	}
	
	public Object extensionValue(String extName) {
		return null;
	}
	
	public PeerSwarm getSwarm() {
		return swarm;
	}

	public String getAddress() {
		return address;
	}

	public int maxPayloadSize() {
		return 64*1024;
	}
	
	public PeerMessageOutgoing makeOutgoingMessage(byte cmd, InputStream txPayload) {
		return new PeerMessageOutgoing(connection, cmd, txPayload);
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
	
	public synchronized int issueMessageId() {
		return nextSendMessageId++;
	}

	public boolean matchesAddress(String address) {
		return getAddress().equals(address);
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
	
	public void finishedMessage(PeerMessageIncoming message) {
		incoming.remove(message.msgId);
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
					
					processMessage(msgId, cmd, flags, payload);
				}
			} catch(ProtocolViolationException exc) {
				violation();
			} catch(Exception exc) {
				logger.error("Socket receive thread for {} caught exception", getAddress(), exc);
				violation();
			}
		}).start();
	}
	
	protected void processMessage(int msgId, byte cmd, byte flags, byte[] payload) {
		PeerMessageIncoming msg;
		if(msgId > maxReceivedMessageId) {
			msg = new PeerMessageIncoming(connection, cmd, flags, msgId);
			synchronized(this) {
				incoming.put(msgId, msg);
				maxReceivedMessageId = msgId;
			}
			pruneMessages();
		} else if(incoming.containsKey(msgId)) {
			msg = incoming.get(msgId);
		} else {
			rejectMessage(msgId);
			return;
		}
		
		msg.receivedData(flags, payload);
	}
	
	protected void pruneMessages() {
		while(incoming.size() > PeerMessage.MAX_OPEN_MESSAGES) {
			int pruneMsgId = -1;
			long oldestTimestamp = Long.MAX_VALUE;
			
			for(PeerMessageIncoming msg : incoming.values()) {
				if(msg.lastSeen < oldestTimestamp) {
					pruneMsgId = msg.msgId;
					oldestTimestamp = msg.lastSeen;
				}
			}
			
			incoming.remove(pruneMsgId);
		}
	}
	
	protected void rejectMessage(int msgId) {
		// TODO: (someday) send response advising peer we don't want this message anymore
	}
	
	protected synchronized void dataReady(MessageSegment segment) {
		ready.add(segment);
		outgoing.notifyAll();
	}
	
	protected void assertState(boolean test) throws ProtocolViolationException {
		if(!test) throw new ProtocolViolationException();
	}
}
