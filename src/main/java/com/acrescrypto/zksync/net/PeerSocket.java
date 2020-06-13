package com.acrescrypto.zksync.net;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.PeerConnection.PeerCapabilityException;
import com.acrescrypto.zksync.net.PeerMessageOutgoing.PeerMessageOutgoingDataProvider;
import com.acrescrypto.zksync.net.dht.BenignProtocolViolationException;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.GroupedThreadPool;
import com.acrescrypto.zksync.utility.Util;

public abstract class PeerSocket {
	public interface PeerSocketReadCallback {
		void received(ByteBuffer data) throws ProtocolViolationException;
	}
	
	public interface PeerSocketWriteCallback {
		void sent() throws IOException, ProtocolViolationException;
	}
	
	public interface PeerSocketHandshakeCallback {
		void ready() throws IOException, ProtocolViolationException;
	}
	
	protected PeerSwarm swarm;
	protected PeerConnection connection;
	protected PublicDHKey remoteIdentityKey;
	protected LinkedList<Integer> recentRejections = new LinkedList<>();
	protected ConcurrentHashMap<Integer,PeerMessageIncoming> incoming = new ConcurrentHashMap<>();
	protected ConcurrentHashMap<Integer,PeerMessageOutgoing> outgoing = new ConcurrentHashMap<>();
	protected GroupedThreadPool threadPool;
	
	protected int nextSendMessageId, maxReceivedMessageId = Integer.MIN_VALUE;
	protected final Logger logger = LoggerFactory.getLogger(PeerSocket.class);
	
	protected String address;
	
	public final static String EXT_FULL_PEER = "EXT_FULL_PEER";

	public abstract PeerAdvertisement getAd();

	public abstract void write(ByteBuffer data, PeerSocketWriteCallback callback) throws IOException;
	public abstract void read(int length, PeerSocketReadCallback callback);
	public abstract boolean isLocalRoleClient();
	protected abstract void _close() throws IOException;
	public abstract boolean isClosed();
	public abstract void handshake(PeerConnection conn, PeerSocketHandshakeCallback callback) throws IOException;
	public abstract int getPeerType() throws UnsupportedOperationException;
	public abstract byte[] getSharedSecret();
	public BandwidthMonitor getMonitorRx() { return null; };
	public BandwidthMonitor getMonitorTx() { return null; };
	
	protected PeerSocket() {}
	
	protected PeerSocket(PeerSwarm swarm) {
		this.swarm = swarm;
		threadPool = GroupedThreadPool.newCachedThreadPool(
				   swarm != null
				 ? swarm.threadPool.threadGroup
				 : Thread.currentThread().getThreadGroup(),
				 "PeerSocket");
	}
	
	public void write(ByteBuffer data) throws IOException {
		this.write(data, null);
	}
	
	public final void close() throws IOException {
		logger.trace("Swarm {} {}:{}: PeerSocket tidying up closed connection",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort());		
		_close();
		try {
			closeAllIncoming();
		} catch(ProtocolViolationException exc) {
			// Probably can't happen, but if it does, then we have to blacklist
			logger.error("Swarm {} {}:{}: Encountered protocol violation closing socket; blacklisting",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					exc);
			violation();
		}
		closeAllOutgoing();
		threadPool.shutdownNow();
	}
	
	public void safeClose() {
		try {
			close();
		} catch(IOException exc) {
			logger.error("Swarm {} {}:{}: Encountered exception closing socket",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					exc);	
		}
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
	
	public PeerMessageOutgoing makeOutgoingMessage(byte cmd) {
		return new PeerMessageOutgoing(connection, cmd);
	}
	
	public PeerMessageOutgoing makeOutgoingMessage(byte cmd, ByteBuffer payload) throws IOException {
		PeerMessageOutgoing msg = new PeerMessageOutgoing(connection, cmd);
		msg.send(payload, false);
		return msg;
	}
	
	public PeerMessageOutgoing makeOutgoingMessage(byte cmd, PeerMessageOutgoingDataProvider dataProvider) throws IOException {
		// TODO: we do still need to track outgoing messages for future cancellation
		PeerMessageOutgoing msg = new PeerMessageOutgoing(
				connection, 
				cmd,
				dataProvider);
		msg.transmit();
		return msg;
	}
	
	public synchronized void finishedMessage(PeerMessageIncoming message) throws IOException {
		logger.debug("Swarm {} {}:{}: PeerSocket finished incoming message {}",
				Util.formatArchiveId(swarm.config.getArchiveId()),
				getAddress(),
				getPort(),
				message.msgId);
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
	
	protected void expectNextMessage() {
		read(PeerMessageIncoming.HEADER_LENGTH, (header)->{
			int  msgId = header.getInt();
			int  len   = header.getInt();
			byte cmd   = header.get   ();
			byte flags = header.get   ();
			
			assertState(  0 <= len);
			assertState(len <= maxPayloadSize());
			
			read(len, (payload)->{
				handleMessage(msgId, cmd, flags, payload);
				expectNextMessage();
			});
		});
	}
	
	protected void handleMessage(int msgId, byte cmd, byte flags, ByteBuffer payload) throws ProtocolViolationException {
		try {
			try {
				processMessage(msgId, cmd, flags, payload);
			} catch(SocketException|EOFException exc) { // socket closed; just ignore it
				logger.debug("Swarm {} {}:{}: PeerSocket unable to read socket, closed={}",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						isClosed(),
						exc);
				close();
			} catch(BenignProtocolViolationException exc) {
				logger.debug("Swarm {} {}:{}: PeerSocket caught worrisome, but not obviously malevolent protocol violation in socket receive thread; closing socket",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						exc);
				close();
			} catch(Exception exc) {					
				logger.debug("Swarm {} {}:{}: PeerSocket caught exception in socket receive thread",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						exc);
				violation();
			}
		} catch(IOException exc) {
			logger.debug("Swarm {} {}:{}: PeerSocket encountered exception closing socket {}",
					Util.formatArchiveId(swarm.config.getArchiveId()),
					getAddress(),
					getPort(),
					exc);
		}
	}
	
	protected void processMessage(int msgId, byte cmd, byte flags, ByteBuffer payload) throws IOException, ProtocolViolationException, PeerCapabilityException {
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
						payload.remaining());
				msg = new PeerMessageIncoming(connection, cmd, flags, msgId);
				msg.begin();
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
						payload.remaining());
				msg = incoming.get(msgId);
			} else { // pruned message
				logger.debug("Swarm {} {}:{}: PeerSocket received continuation of pruned message msgId={}, cmd={}, flags={}, |payload|={}",
						Util.formatArchiveId(swarm.config.getArchiveId()),
						getAddress(),
						getPort(),
						msgId,
						cmd,
						flags,
						payload.remaining());
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
		PeerMessageOutgoing msg = outgoing.get(msgId);
		if(msg == null) return;
		msg.abort();
	}
	
	protected void rejectMessage(int msgId, byte cmd) throws IOException, ProtocolViolationException {
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
		new PeerMessageOutgoing(
				connection,
				msgId,
				cmd,
				PeerMessage.FLAG_CANCEL,
				null
			).transmit();
	}
	
	protected void assertState(boolean test) throws ProtocolViolationException {
		if(!test) throw new ProtocolViolationException();
	}
	
	protected synchronized void closeAllIncoming() throws IOException, ProtocolViolationException {
		for(PeerMessageIncoming msg : incoming.values()) {
			msg.close();
		}
	}
	
	protected synchronized void closeAllOutgoing() {
		for(PeerMessageOutgoing msg : outgoing.values()) {
			msg.abort();
		}
	}
}
