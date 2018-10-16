package com.acrescrypto.zksync.net;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.RejectedExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public abstract class PeerSocket {
	protected PeerSwarm swarm;
	protected PeerConnection connection;
	protected PublicDHKey remoteIdentityKey;
	protected LinkedList<PeerMessageOutgoing> outgoing = new LinkedList<PeerMessageOutgoing>();
	protected LinkedList<MessageSegment> ready = new LinkedList<MessageSegment>();	
	protected HashMap<Integer,PeerMessageIncoming> incoming = new HashMap<Integer,PeerMessageIncoming>();
	protected GroupedThreadPool threadPool;
	
	protected int nextSendMessageId, maxReceivedMessageId = Integer.MIN_VALUE;
	protected final Logger logger = LoggerFactory.getLogger(PeerSocket.class);
	
	protected String address;
	
	public final static String EXT_FULL_PEER = "EXT_FULL_PEER";

	public abstract PeerAdvertisement getAd();

	public abstract void write(byte[] data, int offset, int length) throws IOException, ProtocolViolationException;
	public abstract int read(byte[] data, int offset, int length) throws IOException, ProtocolViolationException;
	public abstract boolean isLocalRoleClient();
	protected abstract void _close() throws IOException;
	public abstract boolean isClosed();
	public abstract void handshake(PeerConnection conn) throws ProtocolViolationException, IOException;
	public abstract int getPeerType() throws UnsupportedOperationException;
	public abstract byte[] getSharedSecret();
	
	public void handshake() throws ProtocolViolationException, IOException { handshake(null); }
	
	protected PeerSocket(PeerSwarm swarm) {
		this.swarm = swarm;
		threadPool = GroupedThreadPool.newCachedThreadPool(swarm != null ? swarm.threadPool.threadGroup : Thread.currentThread().getThreadGroup(), "PeerSocket");
	}
	
	public final void close() throws IOException {
		if(isClosed()) return;
		_close();
		closeAllIncoming();
		closeAllOutgoing();
		threadPool.shutdownNow();
	}
	
	/** Immediately close socket and blacklist due to a clear protocol violation. 
	 * @throws IOException */
	public void violation() {
		logger.warn("Logging violation for peer {}", getAddress());
		try {
			close();
			swarm.config.getAccessor().getMaster().getBlacklist().add(getAddress(), Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
		} catch (IOException exc) {
			logger.warn("Caught exception closing socket to peer {}", getAddress(), exc);
		}
	}

	public int getPort() {
		return -1;
	}
	
	
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
		PeerMessageOutgoing msg = new PeerMessageOutgoing(connection, cmd, txPayload);
		synchronized(outgoing) {
			if(isClosed()) {
				msg.abort();
			} else {
				outgoing.add(msg);
			}
		}
		return msg;
	}
	
	public synchronized void finishedMessage(PeerMessageIncoming message) throws IOException {
		message.rxBuf.setEOF();
		incoming.remove(message.msgId);
	}

	public PeerMessageIncoming messageWithId(int msgId) {
		return incoming.getOrDefault(msgId, null);
	}

	public boolean matchesAddress(String address) {
		return getAddress().equals(address);
	}

	protected synchronized int issueMessageId() {
		return nextSendMessageId++;
	}

	/** Handle some sort of I/O exception */
	protected void ioexception(IOException exc) {
		/* These are probably our fault. There is the possibility that they are caused by malicious actors. */
		logger.warn("Socket for peer {} caught IOException", getAddress(), exc);
		try {
			close();
		} catch (IOException e) {
			logger.warn("Caught exception closing socket to peer {}", getAddress(), exc);
		}
	}
	
	@SuppressWarnings("unlikely-arg-type")
	protected void sendMessage(MessageSegment segment) throws IOException, ProtocolViolationException {
		if(segment.msg.msgId == Integer.MIN_VALUE) segment.assignMsgId(issueMessageId());
		try {
			write(segment.content.array(), 0, segment.content.limit());
		} finally {
			segment.delivered();
		}
		
		if((segment.flags & PeerMessage.FLAG_FINAL) != 0) {
			synchronized(outgoing) {
				outgoing.remove((Integer) segment.msg.msgId);
			}
		}
	}
	
	protected void sendThread() {
		try {
			threadPool.submit(() -> {
				Util.setThreadName("PeerSocket send thread port " + getPort());
				while(!isClosed()) {
					try {
						synchronized(this) {
							while(!ready.isEmpty()) {
								sendMessage(ready.remove());
							}
						}
						
						try {
							synchronized(outgoing) { outgoing.wait(100); }
						} catch (InterruptedException e) {}
					} catch(EOFException exc) {
					} catch (IOException exc) {
						ioexception(exc);
					} catch(Exception exc) {
						logger.error("Socket send thread for peer {} caught exception", getAddress(), exc);
					}
				}
			});
		} catch(RejectedExecutionException exc) {} // thread pool closed -> socket is shut down, no need to send after all
	}
	
	protected void recvThread() {
		if(threadPool.isShutdown()) return;
		threadPool.submit(() -> {
			Util.setThreadName("PeerSocket receive thread " + getPort());
			try {
				while(!isClosed()) {
					ByteBuffer buf = ByteBuffer.allocate(PeerMessage.HEADER_LENGTH);
					read(buf.array(), 0, PeerMessageIncoming.HEADER_LENGTH);
					
					int msgId = buf.getInt();
					int len = buf.getInt();
					byte cmd = buf.get();
					byte flags = buf.get();
					
					assertState(0 <= len && len <= maxPayloadSize());
					
					byte[] payload = new byte[len];
					if(len > 0) {
						read(payload, 0, payload.length);
					}
					
					processMessage(msgId, cmd, flags, payload);
				}
			} catch(ProtocolViolationException exc) {
				violation();
			} catch(SocketException|EOFException exc) { // socket closed; just ignore it
			} catch(Exception exc) {
				logger.error("Socket receive thread for {} caught exception", getAddress(), exc);
				violation();
			}
		});
	}
	
	protected void processMessage(int msgId, byte cmd, byte flags, byte[] payload) throws IOException {
		PeerMessageIncoming msg;
		if((flags & PeerMessage.FLAG_CANCEL) != 0) {
			cancelMessage(msgId);
			return;
		}
		
		synchronized(this) {
			if(msgId > maxReceivedMessageId) { // new message
				msg = new PeerMessageIncoming(connection, cmd, flags, msgId);
				synchronized(this) {
					incoming.put(msgId, msg);
					maxReceivedMessageId = msgId;
					if(isClosed()) {
						finishedMessage(msg);
					}
				}
				pruneMessages();
			} else if(incoming.containsKey(msgId)) { // existing message
				msg = incoming.get(msgId);
			} else { // pruned message
				if(maxReceivedMessageId == Integer.MAX_VALUE) {
					// we can accept no new messages since we've exceeded the limits of the 32-bit ID field; close and force a reconnect
					close();
				}
				
				if((flags & PeerMessage.FLAG_FINAL) == 0) {
					// tell peer we don't want to hear more about this if they intend to send more
					rejectMessage(msgId, cmd);
				}
				return;
			}
		}
		
		msg.receivedData(flags, payload);
	}
	
	protected void pruneMessages() throws IOException {
		while(incoming.size() > PeerMessage.MAX_OPEN_MESSAGES) {
			PeerMessageIncoming pruneMsg = null;
			
			for(PeerMessageIncoming msg : incoming.values()) {
				if(pruneMsg == null || msg.lastSeen < pruneMsg.lastSeen) {
					pruneMsg = msg;
				}
			}
			
			if(pruneMsg != null) {
				finishedMessage(pruneMsg);
			}
		}
	}
	
	protected void cancelMessage(int msgId) {
		synchronized(outgoing) {
			for(PeerMessageOutgoing msg : outgoing) {
				if(msg.msgId == msgId) {
					msg.abort();
				}
			}
		}
	}
	
	protected void rejectMessage(int msgId, byte cmd) {
		new PeerMessageOutgoing(connection, msgId, cmd, PeerMessage.FLAG_CANCEL, new ByteArrayInputStream(new byte[0]));
	}
	
	protected void dataReady(MessageSegment segment) {
		synchronized(this) {
			if(isClosed()) return;
			ready.add(segment);
		}
		
		synchronized(outgoing) { outgoing.notifyAll(); }
	}
	
	protected void assertState(boolean test) throws ProtocolViolationException {
		if(!test) throw new ProtocolViolationException();
	}
	
	protected synchronized void closeAllIncoming() {
		for(PeerMessageIncoming msg : incoming.values()) {
			msg.rxBuf.setEOF();
		}
	}
	
	protected synchronized void closeAllOutgoing() {
		for(PeerMessageOutgoing msg : outgoing) {
			msg.abort();
		}
	}
}
