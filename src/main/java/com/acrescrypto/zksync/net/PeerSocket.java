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
import com.acrescrypto.zksync.net.dht.BenignProtocolViolationException;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public abstract class PeerSocket {
	protected PeerSwarm swarm;
	protected PeerConnection connection;
	protected PublicDHKey remoteIdentityKey;
	protected LinkedList<PeerMessageOutgoing> outgoing = new LinkedList<PeerMessageOutgoing>();
	protected LinkedList<MessageSegment> ready = new LinkedList<MessageSegment>();
	protected LinkedList<Integer> recentRejections = new LinkedList<>();
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
	public BandwidthMonitor getMonitorRx() { return null; };
	public BandwidthMonitor getMonitorTx() { return null; };
	
	public void handshake() throws ProtocolViolationException, IOException { handshake(null); }
	
	protected PeerSocket() {}
	
	protected PeerSocket(PeerSwarm swarm) {
		this.swarm = swarm;
		threadPool = GroupedThreadPool.newCachedThreadPool(swarm != null ? swarm.threadPool.threadGroup : Thread.currentThread().getThreadGroup(), "PeerSocket");
	}
	
	public final void close() throws IOException {
		logger.trace("Swarm {} {}:{}: PeerSocket tidying up closed connection",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort());		
		_close();
		closeAllIncoming();
		closeAllOutgoing();
		threadPool.shutdownNow();
	}
	
	/** Immediately close socket and blacklist due to a clear protocol violation. 
	 * @throws IOException */
	public void violation() {
		logger.warn("Swarm {} {}:{}: PeerSocket logging protocol violation",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort());
		connection.retryOnClose = false;
		try {
			close();
			swarm.config.getAccessor().getMaster().getBlacklist().add(getAddress(), Blacklist.DEFAULT_BLACKLIST_DURATION_MS);
		} catch (IOException exc) {
			logger.warn("Swarm {} {}:{}: PeerSocket caught exception closing socket",
					Util.formatArchiveId(swarm.getConfig().getArchiveId()),
					getAddress(),
					getPort(),
					exc);
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
				// TODO API: (coverage) branch
				msg.abort();
			} else {
				outgoing.add(msg);
			}
		}
		return msg;
	}
	
	public synchronized void finishedMessage(PeerMessageIncoming message) throws IOException {
		logger.debug("Swarm {} {}:{}: PeerSocket finished incoming message {}",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort(),
				message.msgId);
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
		logger.debug("Swarm {} {}:{}: PeerSocket caught IOException",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort(),
				exc);
		try {
			close();
		} catch (IOException e) {
			logger.warn("Swarm {} {}:{}: PeerSocket caught exception closing socket",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					exc);
		}
	}
	
	protected void sendMessage(MessageSegment segment) throws IOException, ProtocolViolationException {
		if(segment.msg.msgId == Integer.MIN_VALUE) segment.assignMsgId(issueMessageId());
		try {
			write(segment.content.array(), 0, segment.content.limit());
		} finally {
			segment.delivered();
		}
		
		if((segment.flags & PeerMessage.FLAG_FINAL) != 0) {
			synchronized(outgoing) {
				outgoing.remove(segment.msg);
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
						logger.error("Swarm {} {}:{}: PeerSocket send thread caught exception",
								Util.formatArchiveId(swarm.config.getArchiveId()),
								getAddress(),
								getPort(),
								exc);
					}
				}
			});
		} catch(RejectedExecutionException exc) {} // thread pool closed -> socket is shut down, no need to send after all
	}
	
	protected void recvThread() {
		try {
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
				} catch(BenignProtocolViolationException exc) {
					logger.info("Swarm {} {}:{}: PeerSocket caught suspicious protocol violation; closing socket",
							Util.formatArchiveId(swarm.config.getArchiveId()),
							getAddress(),
							getPort(),
							exc);
					try {
						close();
					} catch (IOException exc2) {
						logger.debug("Swarm {} {}:{}: PeerSocket encountered exception closing socket {}",
								Util.formatArchiveId(swarm.config.getArchiveId()),
								getAddress(),
								getPort(),
								exc2);
					}
				} catch(ProtocolViolationException exc) {
					logger.info("Swarm {} {}:{}: PeerSocket caught unacceptable protocol violation; blacklisting peer",
							Util.formatArchiveId(swarm.config.getArchiveId()),
							getAddress(),
							getPort(),
							exc);
					violation();
				} catch(SocketException|EOFException exc) { // socket closed; just ignore it
					logger.debug("Swarm {} {}:{}: PeerSocket unable to read socket, closed={}",
							Util.formatArchiveId(swarm.config.getArchiveId()),
							getAddress(),
							getPort(),
							isClosed(),
							exc);
					try {
						close();
					} catch (IOException exc2) {
						logger.debug("Swarm {} {}:{}: PeerSocket encountered exception closing socket {}",
								Util.formatArchiveId(swarm.config.getArchiveId()),
								getAddress(),
								getPort(),
								exc2);
					}
				} catch(Exception exc) {					
					logger.debug("Swarm {} {}:{}: PeerSocket caught exception in socket receive thread",
							Util.formatArchiveId(swarm.config.getArchiveId()),
							getAddress(),
							getPort(),
							exc);
					violation();
				}
			});
		} catch(RejectedExecutionException exc) {} // socket was shut down, so nothing to receive anyway
	}
	
	protected void processMessage(int msgId, byte cmd, byte flags, byte[] payload) throws IOException {
		// TODO: these log statements are HUGE!!! factor that out...
		PeerMessageIncoming msg;
		if((flags & PeerMessage.FLAG_CANCEL) != 0) {
			cancelMessage(msgId);
			return;
		}
		
		synchronized(this) {
			if(msgId > maxReceivedMessageId) { // new message
				logger.trace("Swarm {} {}:{}: PeerSocket received new message msgId={}, cmd={}, flags={}, |payload|={}",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						msgId,
						cmd,
						flags,
						payload.length);
				msg = new PeerMessageIncoming(connection, cmd, flags, msgId);
				synchronized(this) {
					incoming.put(msgId, msg);
					maxReceivedMessageId = msgId;
					if(isClosed()) {
						logger.debug("Swarm {} {}:{}: PeerSocket pruning msgId={} since socket is closed",
								Util.formatArchiveId(swarm.config.getArchiveId()),
								getAddress(),
								getPort(),
								msgId);
						finishedMessage(msg);
					}
				}
				pruneMessages();
			} else if(incoming.containsKey(msgId)) { // existing message
				logger.trace("Swarm {} {}:{}: PeerSocket received continued message msgId={}, cmd={}, flags={}, |payload|={}",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						msgId,
						cmd,
						flags,
						payload.length);
				msg = incoming.get(msgId);
			} else { // pruned message
				logger.debug("Swarm {} {}:{}: PeerSocket received continuation of pruned message msgId={}, cmd={}, flags={}, |payload|={}",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						msgId,
						cmd,
						flags,
						payload.length);
				if(maxReceivedMessageId == Integer.MAX_VALUE) {
					// we can accept no new messages since we've exceeded the limits of the 32-bit ID field; close and force a reconnect
					logger.info("Swarm {} {}:{}: PeerSocket terminating connection due to maximum message count being reached",
							Util.formatArchiveId(swarm.config.getArchiveId()),
							getAddress(),
							getPort());
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
		int limit = swarm.getConfig().getMaster().getGlobalConfig().getInt("net.swarm.rejectionCacheSize");
		while(incoming.size() > limit) {
			PeerMessageIncoming pruneMsg = null;
			
			for(PeerMessageIncoming msg : incoming.values()) {
				if(pruneMsg == null || msg.lastSeen < pruneMsg.lastSeen) {
					pruneMsg = msg;
				}
			}
			
			if(pruneMsg != null) {
				logger.debug("Swarm {} {}:{}: PeerSocket pruning incoming message {}; limit {} incoming, have {}",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						pruneMsg.msgId,
						limit,
						incoming.size());
				finishedMessage(pruneMsg);
			}
		}
	}
	
	protected void cancelMessage(int msgId) {
		logger.debug("Swarm {} {}:{}: PeerSocket cancelling outgoing message {}",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort(),
				msgId);
		synchronized(outgoing) {
			for(PeerMessageOutgoing msg : outgoing) {
				if(msg.msgId == msgId) {
					msg.abort();
				}
			}
		}
	}
	
	protected void rejectMessage(int msgId, byte cmd) {
		if(recentRejections.contains(msgId)) return;
		int maxSize = swarm.config.getMaster().getGlobalConfig().getInt("net.swarm.rejectionCacheSize");

		synchronized(this) {
			if(recentRejections.contains(msgId)) return;
			recentRejections.add(msgId);
			while(recentRejections.size() > maxSize) {
				recentRejections.poll();
			}
		}

		logger.debug("Swarm {} {}:{}: PeerSocket rejecting message {}",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort(),
				msgId);
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
