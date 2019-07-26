package com.acrescrypto.zksync.net;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SearchFailedException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.exceptions.SwarmTimeoutException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.net.PageQueue.ChunkReference;
import com.acrescrypto.zksync.utility.AppendableInputStream;
import com.acrescrypto.zksync.utility.Util;

public class PeerConnection {
	public final static boolean DISABLE_TAG_LIST = false;
	
	public final static byte CMD_ACCESS_PROOF = 0x00;
	public final static byte CMD_ANNOUNCE_PEERS = 0x01;
	public final static byte CMD_ANNOUNCE_SELF_AD = 0x02;
	public final static byte CMD_ANNOUNCE_TAGS = 0x03;
	public final static byte CMD_ANNOUNCE_TIPS = 0x04;
	public final static byte CMD_ANNOUNCE_REVISION_DETAILS = 0x05;
	public final static byte CMD_REQUEST_ALL = 0x06;
	public final static byte CMD_REQUEST_ALL_CANCEL = 0x07;
	public final static byte CMD_REQUEST_INODES = 0x08;
	public final static byte CMD_REQUEST_REVISION_CONTENTS = 0x09;
	public final static byte CMD_REQUEST_REVISION_STRUCTURE = 0x0a;
	public final static byte CMD_REQUEST_REVISION_DETAILS = 0x0b;
	public final static byte CMD_REQUEST_PAGE_TAGS = 0x0c;
	public final static byte CMD_SEND_PAGE = 0x0d;
	public final static byte CMD_SET_PAUSED = 0x0e;
	
	public final static int MAX_SUPPORTED_CMD = CMD_SET_PAUSED; // update to largest acceptable command code
	
	public final static int PEER_TYPE_STATIC = 0; // static fileserver; needs subclass to handle
	public final static int PEER_TYPE_BLIND = 1; // has knowledge of seed key, but not archive passphrase; can't decipher data
	public final static int PEER_TYPE_FULL = 2; // live peer with knowledge of archive passphrase
	
	/** A message can't be sent to the remote peer because this channel hasn't established that we have full read
	 * access to the archive. */
	public class PeerCapabilityException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	protected PeerSocket socket;
	protected HashSet<Long> announcedTags = new HashSet<Long>(); // TODO Someday: (review) unbounded memory use to track announced page tags
	protected boolean remotePaused, localPaused;
	protected PageQueue queue;
	protected boolean closed;
	protected final Logger logger = LoggerFactory.getLogger(PeerConnection.class);
	protected long timeStart;
	protected boolean wantsEverything;
	protected boolean retryOnClose;
	
	public PeerConnection(PeerSwarm swarm, PeerAdvertisement ad) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		this.socket = PeerSocket.connectToAd(swarm, ad);
		this.socket.handshake(this);
		initialize();
		announceSelf();
	}
	
	public PeerConnection(PeerSocket socket) throws IOException {
		this.socket = socket;
		initialize();
	}
	
	protected void initialize() throws IOException {
		this.retryOnClose = true;
		timeStart = Util.currentTimeMillis();
		socket.connection = this;
		this.queue = new PageQueue(socket.swarm.config);
		socket.threadPool.submit(()->pageQueueThread());
		announceTips();
		announceTags();
	}
	
	protected PeerConnection() {}
	
	public PeerSocket getSocket() {
		return socket;
	}
	
	public int getPeerType() {
		return socket.getPeerType();
	}
	
	public void blacklist() {
		socket.violation();
	}

	public void close() {
		synchronized(this) {
			if(closed) return;
			closed = true;
		}
		
		if(queue != null) {
			queue.close();
		}
		
		try {
			if(socket != null) {
				logger.debug("Swarm {} {}:{}: PeerConnection closing",
						Util.formatArchiveId(socket.swarm.config.getArchiveId()),
						socket.getAddress(),
						socket.getPort());
				socket.close();
			}
		} catch(IOException exc) {
			logger.warn("Swarm {} {}:{}: PeerConnection caught exception closing socket",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					exc);
		} finally {
			if(socket != null && socket.swarm != null) {
				socket.swarm.closedConnection(this);
			}
			
			synchronized(this) {
				this.notifyAll();
			}
		}
	}
	
	/** Announce a tag to the remote peer, and automatically send the page if the remote peer is requesting all data.
	 * Note that the alternative forms of announceTags do NOT automatically send pages to the remote peer at this time.
	 */
	public void announceTag(long shortTag) {
		if(DISABLE_TAG_LIST) return;
		logger.trace("Swarm {} {}:{}: PeerConnection send announceTag",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		ByteBuffer tag = ByteBuffer.allocate(RefTag.REFTAG_SHORT_SIZE);
		tag.putLong(shortTag);
		send(CMD_ANNOUNCE_TAGS, tag.array());
		
		if(wantsEverything && !hasFile(shortTag)) {
			queue.addPageTag(PageQueue.DEFAULT_EVERYTHING_PRIORITY, shortTag);
		}
	}
	
	public void announceSelf() {
		TCPPeerSocketListener listener = socket.swarm.getConfig().getMaster().getTCPListener();
		if(listener == null) return;
		
		TCPPeerAdvertisementListener adListener = listener.listenerForSwarm(socket.swarm);
		if(adListener == null) return;
		
		try {
			// TODO API: (coverage) branch coverage
			announceSelf(adListener.localAd());
		} catch(UnconnectableAdvertisementException exc) {
			logger.error("Swarm {} {}:{}: Unable to announce local ad",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					exc);
		}
	}
	
	public void announceSelf(PeerAdvertisement ad) {
		logger.trace("Swarm {} {}:{}: PeerConnection send announceSelfAd",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		byte[] serializedAd = ad.serialize();
		ByteBuffer serialized = ByteBuffer.allocate(2+serializedAd.length);
		assert(serializedAd.length <= Short.MAX_VALUE);
		serialized.putShort((short) serializedAd.length);
		serialized.put(serializedAd);
		send(CMD_ANNOUNCE_SELF_AD, serialized.array());
	}
	
	public void announcePeer(PeerAdvertisement ad) {
		logger.trace("Swarm {} {}:{}: PeerConnection send announcePeer",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		byte[] serializedAd = ad.serialize();
		ByteBuffer serialized = ByteBuffer.allocate(2+serializedAd.length);
		assert(serializedAd.length <= Short.MAX_VALUE);
		serialized.putShort((short) serializedAd.length);
		serialized.put(serializedAd);
		send(CMD_ANNOUNCE_PEERS, serialized.array());
	}
	
	public void announcePeers(Collection<PeerAdvertisement> ads) {
		logger.trace("Swarm {} {}:{}: PeerConnection send announcePeers",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		byte[][] serializations = new byte[ads.size()][];
		int len = 0, idx = 0;
		for(PeerAdvertisement ad : ads) {
			byte[] serialization = ad.serialize();
			serializations[idx++] = serialization;
			len += serialization.length + 2;
		}
		
		ByteBuffer allSerialization = ByteBuffer.allocate(len);
		for(byte[] serialization : serializations) {
			assert(serialization.length <= Short.MAX_VALUE); // technically this can be 65535, but we should come nowhere close
			allSerialization.putShort((short) serialization.length);
			allSerialization.put(serialization);
		}
		
		send(CMD_ANNOUNCE_PEERS, allSerialization.array());
	}
	
	public void announceShortTags(Collection<Long> tags) {
		if(DISABLE_TAG_LIST) return;
		logger.trace("Swarm {} {}:{}: PeerConnection send announceShortTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		ByteBuffer serialized = ByteBuffer.allocate(tags.size() * RefTag.REFTAG_SHORT_SIZE);
		for(Long shortTag : tags) {
			if(!serialized.hasRemaining()) break; // possible to add tags as we iterate
			serialized.putLong(shortTag);
		}
		send(CMD_ANNOUNCE_TAGS, serialized.array());
	}
	
	public void announceTags(Collection<byte[]> tags) {
		if(DISABLE_TAG_LIST) return;
		logger.trace("Swarm {} {}:{}: PeerConnection send announceTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		ByteBuffer serialized = ByteBuffer.allocate(tags.size() * RefTag.REFTAG_SHORT_SIZE);
		for(byte[] tag : tags) {
			if(!serialized.hasRemaining()) break; // possible to add tags as we iterate
			serialized.putLong(Util.shortTag(tag));
		}
		send(CMD_ANNOUNCE_TAGS, serialized.array());
	}
	
	public void announceTags() {
		if(DISABLE_TAG_LIST) return;
		ZKArchive archive = socket.swarm.config.getArchive();
		if(archive == null) {
			send(CMD_ANNOUNCE_TAGS, new byte[0]);
			return;
		}
		
		announceTags(new ArrayList<>(archive.allPageTags()));
	}
	
	public void announceTip(RevisionTag tip) {
		logger.trace("Swarm {} {}:{}: PeerConnection send announceTip",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		send(CMD_ANNOUNCE_TIPS, tip.getBytes());
	}
	
	public void announceTips() throws IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection send announceTips",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		ZKArchive archive = socket.swarm.config.getArchive();
		if(archive == null || archive.getConfig().getRevisionList() == null) {
			send(CMD_ANNOUNCE_TIPS, new byte[0]);
			return;
		}
		
		Collection<RevisionTag> branchTipsClone = archive.getConfig().getRevisionList().branchTips();
		ByteBuffer buf = ByteBuffer.allocate(branchTipsClone.size() * RevisionTag.sizeForConfig(socket.swarm.config));
		for(RevisionTag tag : branchTipsClone) {
			buf.put(tag.getBytes());
		}

		send(CMD_ANNOUNCE_TIPS, buf.array());
	}
	
	public void announceRevisionDetails(RevisionTag tag) {
		logger.trace("Swarm {} {}:{}: PeerConnection send announceRevisionDetails",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		Collection<RevisionTag> parents = socket.swarm.config.getRevisionTree().parentsForTagLocal(tag);
		if(parents == null) return;
		
		ByteBuffer buf = ByteBuffer.allocate((1+parents.size()) * RevisionTag.sizeForConfig(socket.swarm.config));
		buf.put(tag.getBytes());
		for(RevisionTag parent : parents) {
			buf.put(parent.getBytes());
		}
		
		send(CMD_ANNOUNCE_REVISION_DETAILS, buf.array());
	}
	
	public void requestAll() {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestAll",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		send(CMD_REQUEST_ALL, new byte[0]);
	}
	
	public void requestAllCancel() {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestAllCancel",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		send(CMD_REQUEST_ALL_CANCEL, new byte[0]);
	}
	
	public void requestPageTag(int priority, long shortTag) {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestPageTag",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		Util.debugLog(String.format("PeerConnection %s: Requesting page tag %016x",
				socket.getSwarm().getConfig().getMaster().getName(),
				shortTag));
		ByteBuffer buf = ByteBuffer.allocate(4+RefTag.REFTAG_SHORT_SIZE);
		buf.putInt(priority);
		buf.putLong(shortTag);
		send(CMD_REQUEST_PAGE_TAGS, buf.array());
	}

	public void requestPageTags(int priority, Collection<Long> pageTags) {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestPageTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(pageTags.isEmpty()) return;
		ByteBuffer pageTagsMerged = ByteBuffer.allocate(4+RefTag.REFTAG_SHORT_SIZE*pageTags.size());
		pageTagsMerged.putInt(priority);
		for(Long shortTag : pageTags) {
			pageTagsMerged.putLong(shortTag);
		}
		
		send(CMD_REQUEST_PAGE_TAGS, pageTagsMerged.array());
	}
	
	/** Request encrypted files pertaining to a given inode (including page tree chunks). */
	public void requestInodes(int priority, RevisionTag revTag, Collection<Long> inodeIds) throws PeerCapabilityException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestInodes",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(inodeIds.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		
		ByteBuffer buf = ByteBuffer.allocate(4 + revTag.getBytes().length + 8*inodeIds.size());
		buf.putInt(priority);
		buf.put(revTag.getBytes());
		for(long inodeId: inodeIds) {
			buf.putLong(inodeId);
		}
		
		send(CMD_REQUEST_INODES, buf.array());
	}
	
	public void requestRevisionContents(int priority, Collection<RevisionTag> tips) throws PeerCapabilityException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestRevisionContents",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(tips.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_CONTENTS, serializeRevTags(priority, tips));
	}

	public void requestRevisionStructure(int priority, Collection<RevisionTag> tips) throws PeerCapabilityException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestRevisionStructure",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(tips.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_STRUCTURE, serializeRevTags(priority, tips));
	}

	public void requestRevisionDetails(int priority, RevisionTag revTag) throws PeerCapabilityException {
		ArrayList<RevisionTag> list = new ArrayList<>();
		list.add(revTag);
		requestRevisionDetails(priority, list);
	}
	
	public void requestRevisionDetails(int priority, Collection<RevisionTag> revTags) throws PeerCapabilityException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestRevisionDetails",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(revTags.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_DETAILS, serializeRevTags(priority, revTags));
	}
	
	public void setPaused(boolean paused) {
		logger.trace("Swarm {} {}:{}: PeerConnection send setPaused",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		send(CMD_SET_PAUSED, new byte[] { (byte) (paused ? 0x01 : 0x00) });
	}
	
	public synchronized void setLocalPaused(boolean localPaused) {
		logger.trace("Swarm {} {}:{}: PeerConnection send setLocalPaused",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		this.localPaused = localPaused;
		this.notifyAll();
	}

	public boolean isPausable(byte cmd) {
		return cmd == CMD_SEND_PAGE;
	}

	public boolean isPaused() {
		return remotePaused || localPaused;
	}

	public synchronized void waitForUnpause() throws SocketClosedException {
		while(isPaused() && !closed) {
			try {
				this.wait();
			} catch(InterruptedException e) {}
			if(!closed) assertConnected();
		}
	}

	public boolean wantsFile(byte[] tag) {
		return !announcedTags.contains(Util.shortTag(tag));
	}

	public boolean hasFile(long shortTag) {
		return announcedTags.contains(shortTag);
	}
	
	protected void waitForFullInit() throws EOFException {
		Util.blockOn(()->!closed && !socket.swarm.config.hasKey() && !socket.swarm.config.getAccessor().isSeedOnly());
		Util.blockOn(()->!closed && socket.swarm.config.getArchive() == null);
		if(closed) throw new EOFException(); // maybe not best exception, but we just want to stop processing without triggering a blacklist
	}
	
	public boolean handle(PeerMessageIncoming msg) throws ProtocolViolationException {
		logger.debug("Swarm {} {}:{}: PeerConnection handler starting for msgId {}, cmd {}",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort(),
				msg.msgId,
				msg.cmd);
		try {
			switch(msg.cmd) {
			case CMD_ANNOUNCE_PEERS:
				handleAnnouncePeers(msg);
				break;
			case CMD_ANNOUNCE_SELF_AD:
				handleAnnounceSelfAd(msg);
				break;
			case CMD_ANNOUNCE_TAGS:
				handleAnnounceTags(msg);
				break;
			case CMD_ANNOUNCE_TIPS:
				handleAnnounceTips(msg);
				break;
			case CMD_ANNOUNCE_REVISION_DETAILS:
				handleAnnounceRevisionDetails(msg);
				break;
			case CMD_REQUEST_ALL:
				handleRequestAll(msg);
				break;
			case CMD_REQUEST_ALL_CANCEL:
				handleRequestAllCancel(msg);
				break;
			case CMD_REQUEST_INODES:
				handleRequestInodes(msg);
				break;
			case CMD_REQUEST_REVISION_CONTENTS:
				handleRequestRevisionContents(msg);
				break;
			case CMD_REQUEST_REVISION_STRUCTURE:
				handleRequestRevisionStructure(msg);
				break;
			case CMD_REQUEST_REVISION_DETAILS:
				handleRequestRevisionDetails(msg);
				break;
			case CMD_REQUEST_PAGE_TAGS:
				handleRequestPageTags(msg);
				break;
			case CMD_SEND_PAGE:
				handleSendPage(msg);
				break;
			case CMD_SET_PAUSED:
				handleSetPaused(msg);
				break;
			default:
				logger.info("Swarm {} {}:{}: PeerConnection handler ignoring unknown request command ",
						Util.formatArchiveId(socket.swarm.config.getArchiveId()),
						socket.getAddress(),
						socket.getPort(),
						msg.cmd);
				return false;
			}
			
			logger.trace("Swarm {} {}:{}: PeerConnection handler finished cleanly for msgId {}, cmd {}",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					msg.msgId,
					msg.cmd);
		} catch(EOFException exc) {
			// ignore these
			logger.trace("Swarm {} {}:{}: PeerConnection handler EOF for msgId {}, cmd {}",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					msg.msgId,
					msg.cmd);
		} catch(PeerCapabilityException | IOException | InvalidSignatureException | SecurityException exc) {
			/* Arguably, blacklisting people because we had a local IOException is unfair. But, there are two real
			 * possibilities here:
			 *   1. They're doing something nasty that's triggering IOExceptions. THey deserve it.
			 *   2. They're not doing anything nasty. In which case, we're getting IOExceptions doing normal stuff, so
			 *      it's not like we can participate as peers right now anyway.
			 * Sadly, there's the possibility that we cut ourselves off for a long time because we had a volume go
			 * unmounted or something. That's not great. An opportunity for future improvement...
			 */
			logger.warn("Swarm {} {}:{}: PeerConnection handler triggered exception for msgId {}, cmd {}; blacklisting",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					msg.msgId,
					msg.cmd,
					exc);
			throw new ProtocolViolationException();
		}
		
		return true;
	}

	protected void handleAnnouncePeers(PeerMessageIncoming msg) throws EOFException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv announcePeers",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		while(msg.rxBuf.hasRemaining()) {
			int adLen = Util.unsignShort(msg.rxBuf.getShort());
			assertState(0 < adLen && adLen <= PeerMessage.MESSAGE_SIZE);
			byte[] adRaw = new byte[adLen];
			msg.rxBuf.get(adRaw);
			CryptoSupport crypto = socket.swarm.config.getAccessor().getMaster().getCrypto();
			
			try {
				PeerAdvertisement ad = PeerAdvertisement.deserializeRecord(crypto, ByteBuffer.wrap(adRaw));
				if(ad == null) continue;
				if(ad.isBlacklisted(socket.swarm.config.getAccessor().getMaster().getBlacklist())) continue;
				socket.swarm.addPeerAdvertisement(ad);
			} catch (IOException | UnconnectableAdvertisementException e) {
			}
		}
	}
	
	protected void handleAnnounceSelfAd(PeerMessageIncoming msg) throws ProtocolViolationException, EOFException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceSelfAd",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		int adLen = Util.unsignShort(msg.rxBuf.getShort());
		assertState(0 < adLen && adLen <= PeerMessage.MESSAGE_SIZE);
		byte[] adRaw = new byte[adLen];
		msg.rxBuf.get(adRaw);
		try {
			PeerAdvertisement ad = PeerAdvertisement.deserializeRecordWithAddress(msg.connection.getCrypto(), ByteBuffer.wrap(adRaw), msg.connection.socket.getAddress());
			if(ad != null && !ad.isBlacklisted(socket.swarm.config.getAccessor().getMaster().getBlacklist())) {
				socket.swarm.addPeerAdvertisement(ad);
			}
		} catch (IOException | UnconnectableAdvertisementException e) {
		}
		
		msg.rxBuf.requireEOF();
	}
	
	protected void handleAnnounceTags(PeerMessageIncoming msg) throws EOFException {
		if(DISABLE_TAG_LIST) return;
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		assert(RefTag.REFTAG_SHORT_SIZE == 8); // This code depends on tags being sent as 64-bit values.
		while(msg.rxBuf.hasRemaining()) {
			// lots of tags to go through, and locks are expensive; accumulate into a buffer so we can minimize lock/release cycling
			Util.blockOn(()->msg.rxBuf.available() < 8 && !msg.rxBuf.isEOF());
			if(msg.rxBuf.isEOF() && msg.rxBuf.available() < 8) break;
			
			int len = Math.min(64*1024, msg.rxBuf.available());
			ByteBuffer buf = ByteBuffer.allocate(len - len % 8); // round to 8-byte long boundary
			msg.rxBuf.get(buf.array());
			synchronized(this) {
				while(buf.hasRemaining()) {
					long shortTag = buf.getLong();
					announcedTags.add(shortTag);
				}
			}
		}
		
		synchronized(this) {
			this.notifyAll();
		}
	}
	
	protected void handleAnnounceTips(PeerMessageIncoming msg) throws InvalidSignatureException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceTips",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		byte[] revTagRaw = new byte[RevisionTag.sizeForConfig(socket.swarm.config)];
		while(msg.rxBuf.hasRemaining()) {
			msg.rxBuf.get(revTagRaw);
			RevisionTag revTag = new RevisionTag(socket.swarm.config, revTagRaw, false);
			try {
				logger.debug("Swarm {} {}:{}: PeerConnection announceTips considering revtag {}",
						Util.formatArchiveId(socket.swarm.config.getArchiveId()),
						socket.getAddress(),
						socket.getPort(),
						Util.formatRevisionTag(revTag));
				if(socket.swarm.config.getRevisionList().addBranchTip(revTag, true)) {
					logger.info("Swarm {} {}:{}: PeerConnection announceTips adopted new revTag {}",
							Util.formatArchiveId(socket.swarm.config.getArchiveId()),
							socket.getAddress(),
							socket.getPort(),
							Util.formatRevisionTag(revTag));
				}
			} catch(SearchFailedException exc) {
				/* placeholder if we ever want to do something about getting a branch tip we can't get info about
				** kinda weird that we didn't learn anything, since the peer who told us about it ought to have info...*/ 
			} catch(IOException exc) {
			}
		}
		
		socket.swarm.config.getRevisionList().write();
	}
	
	protected void handleAnnounceRevisionDetails(PeerMessageIncoming msg) throws PeerCapabilityException, EOFException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceRevisionDetails",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		assertPeerCapability(PEER_TYPE_FULL);

		byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(socket.swarm.config)];
		LinkedList<RevisionTag> parents = new LinkedList<>();
		RevisionTag revTag = new RevisionTag(socket.swarm.config, msg.rxBuf.read(revTagBytes), true);
		
		while(msg.rxBuf.hasRemaining()) {
			byte[] parentBytes = new byte[revTagBytes.length];
			RevisionTag parent = new RevisionTag(socket.swarm.config, msg.rxBuf.read(parentBytes), true);
			parents.add(parent);
		}
		
		socket.swarm.config.getRevisionTree().addParentsForTag(revTag, parents);
	}
	
	protected void handleRequestAll(PeerMessageIncoming msg) throws ProtocolViolationException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestAll",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		msg.rxBuf.requireEOF();
		sendEverything();
	}

	protected void handleRequestAllCancel(PeerMessageIncoming msg) throws ProtocolViolationException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestAllCancel",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		msg.rxBuf.requireEOF();
		stopSendingEverything();
	}

	protected void handleRequestInodes(PeerMessageIncoming msg) throws PeerCapabilityException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestInodes",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		ZKArchive archive = socket.swarm.config.getArchive();
		
		assertPeerCapability(PEER_TYPE_FULL);
		byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(archive.getConfig())];
		int priority = msg.rxBuf.getInt();
		RevisionTag tag;
		try {
			tag = new RevisionTag(archive.getConfig(), msg.rxBuf.read(revTagBytes), false);
		} catch(SecurityException exc) {
			return;
		}
		
		while(msg.rxBuf.hasRemaining()) {
			long inodeId = msg.rxBuf.getLong();
			sendInodeContents(priority, tag, inodeId);
		}
	}
	
	protected void handleRequestRevisionContents(PeerMessageIncoming msg) throws PeerCapabilityException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestRevisionContents",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		ZKArchive archive = socket.swarm.config.getArchive();

		assertPeerCapability(PEER_TYPE_FULL);
		int priority = msg.rxBuf.getInt();
		
		while(msg.rxBuf.hasRemaining()) {
			byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(archive.getConfig())];
			msg.rxBuf.read(revTagBytes);
			RevisionTag tag = new RevisionTag(archive.getConfig(), revTagBytes, false);
			sendRevisionContents(priority, tag);
		}
	}
	
	protected void handleRequestRevisionStructure(PeerMessageIncoming msg) throws PeerCapabilityException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestRevisionStructure",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		ZKArchive archive = socket.swarm.config.getArchive();

		assertPeerCapability(PEER_TYPE_FULL);
		int priority = msg.rxBuf.getInt();
		
		while(msg.rxBuf.hasRemaining()) {
			byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(archive.getConfig())];
			msg.rxBuf.read(revTagBytes);
			RevisionTag tag = new RevisionTag(archive.getConfig(), revTagBytes, false);
			sendRevisionStructure(priority, tag);
		}
	}
	
	protected void handleRequestRevisionDetails(PeerMessageIncoming msg) throws PeerCapabilityException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestRevisionDetails",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		ZKArchive archive = socket.swarm.config.getArchive();

		assertPeerCapability(PEER_TYPE_FULL);
		// TODO Someday: (implement) honor prioritization requests for revision details, or drop priority field altogether
		msg.rxBuf.getInt(); // this is a priority value, which we don't actually implement support for yet
		
		byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(archive.getConfig())];
		while(msg.rxBuf.hasRemaining()) {
			RevisionTag tag = new RevisionTag(archive.getConfig(), msg.rxBuf.read(revTagBytes), false);
			announceRevisionDetails(tag);
		}
	}
	
	protected void handleRequestPageTags(PeerMessageIncoming msg) throws IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestPageTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		waitForFullInit();
		byte[] shortTag = new byte[RefTag.REFTAG_SHORT_SIZE];
		int priority = msg.rxBuf.getInt();
		while(msg.rxBuf.hasRemaining()) {
			msg.rxBuf.get(shortTag);
			sendPageTag(priority, ByteBuffer.wrap(shortTag).getLong());
		}
	}
	
	protected void handleSendPage(PeerMessageIncoming msg) throws IOException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv sendPage {}",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort(),
				msg.msgId);
		byte[] tag = msg.rxBuf.read(socket.swarm.config.getCrypto().hashLength());
		if(!Arrays.equals(tag, socket.swarm.getConfig().tag())) {
			waitForFullInit();
		}
		
		if(socket.swarm.getConfig().getCacheStorage().exists(Page.pathForTag(tag))) {
			logger.trace("Swarm {} {}:{}: PeerConnection handleSendPage {} ignoring offered page {} (already have it)",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					msg.msgId,
					Util.bytesToHex(tag, 8));
			if(!DISABLE_TAG_LIST) {
				announceTag(Util.shortTag(tag));
			}
			return;
		}
		
		logger.trace("Swarm {} {}:{}: PeerConnection handleSendPage {} receiving page {}",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort(),
				msg.msgId,
				Util.bytesToHex(tag, 8));
		ChunkAccumulator accumulator = socket.swarm.accumulatorForTag(tag);		
		int actualPageSize = socket.swarm.config.getSerializedPageSize();
		int expectedChunks = (int) Math.ceil(((double) actualPageSize)/PeerMessage.FILE_CHUNK_SIZE);
		int finalChunkSize = actualPageSize % PeerMessage.FILE_CHUNK_SIZE;
		
		while(!accumulator.isFinished() && msg.rxBuf.hasRemaining()) {
			long offset = Util.unsignInt(msg.rxBuf.getInt());
			assertState(0 <= offset && offset < expectedChunks && offset <= Integer.MAX_VALUE,
					String.format("Swarm %s %s:%s: PeerConnection handleSendPage %d invalid offset offset=%d, expected 0 <= offset < %d <= %d",
							Util.formatArchiveId(socket.swarm.config.getArchiveId()),
							socket.getAddress(),
							socket.getPort(),
							msg.msgId,
							offset,
							expectedChunks,
							Integer.MAX_VALUE));
			int readLen = offset == expectedChunks - 1 ? finalChunkSize : PeerMessage.FILE_CHUNK_SIZE;
			byte[] chunkData = msg.rxBuf.read(readLen);
			logger.trace("Swarm {} {}:{}: PeerConnection handleSendPage {} received page {} chunk {}",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					msg.msgId,
					Util.bytesToHex(tag, 8),
					offset);
			accumulator.addChunk((int) offset, chunkData, this);
		}
	}
	
	protected void handleSetPaused(PeerMessageIncoming msg) throws ProtocolViolationException, EOFException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv setPaused",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		byte pausedByte = msg.rxBuf.get();
		assertState(pausedByte == 0x00 || pausedByte == 0x01);
		msg.rxBuf.requireEOF();
		setRemotePaused(pausedByte == 0x01);
	}
	
	protected void send(byte cmd, byte[] payload) {
		assert(0 <= cmd && cmd <= Byte.MAX_VALUE);
		socket.makeOutgoingMessage(cmd, new ByteArrayInputStream(payload));
	}
	
	protected synchronized void setRemotePaused(boolean paused) {
		this.remotePaused = paused;
		this.notifyAll();
	}
	
	protected void sendEverything() throws IOException {
		wantsEverything = true;
		queue.startSendingEverything();
	}

	protected void stopSendingEverything() throws IOException {
		queue.stopSendingEverything();
	}

	protected void sendRevisionContents(int priority, RevisionTag revTag) throws IOException {
		queue.addRevisionTag(priority, revTag);
	}
	
	protected void sendRevisionStructure(int priority, RevisionTag revTag) throws IOException {
		queue.addRevisionTagForStructure(priority, revTag);
	}
	
	protected void sendInodeContents(int priority, RevisionTag revTag, long inodeId) throws IOException {
		queue.addInodeContents(priority, revTag, inodeId);
	}
	
	protected void sendPageTag(int priority, long shortTag) throws IOException {
		queue.addPageTag(priority, shortTag);
	}
	
	protected void assertConnected() throws SocketClosedException {
		if(socket.isClosed()) {
			throw new SocketClosedException();
		}
	}

	protected byte[] serializeRevTags(int priority, Collection<RevisionTag> tags) {
		ByteBuffer buf = ByteBuffer.allocate(4 + tags.size() * RevisionTag.sizeForConfig(socket.swarm.config));
		buf.putInt(priority);
		for(RevisionTag tag : tags) {
			buf.put(tag.getBytes());
		}
		return buf.array();
	}

	protected void assertPeerCapability(int capability) throws PeerCapabilityException {
		if(socket.getPeerType() < capability) throw new PeerCapabilityException();
	}

	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
	
	protected void assertState(boolean state, String message) throws ProtocolViolationException {
		if(!state) {
			logger.warn(message);
			throw new ProtocolViolationException();
		}
	}
	
	protected void pageQueueThread() {
		Util.setThreadName("PeerConnection queue thread");
		byte[] lastTag = new byte[0];
		AppendableInputStream lastStream = null;
		
		while(!socket.isClosed()) {
			try {
				ChunkReference chunk = queue.nextChunk();
				if(chunk == null || !wantsFile(chunk.tag.getBytes())) continue;
				waitForUnpause();
				
				if(lastStream == null || !Arrays.equals(lastTag, chunk.tag.getBytes())) {
					if(lastStream != null) {
						lastStream.eof();
					}
					
					lastStream = new AppendableInputStream();
					socket.makeOutgoingMessage(CMD_SEND_PAGE, lastStream);
					lastStream.write(chunk.tag.getBytes());
					lastTag = chunk.tag.getBytes();
				}
				
				logger.trace("Swarm {} {}:{}: PeerConnection page queue thread sending chunk {} of tag {}",
						Util.formatArchiveId(socket.swarm.config.getArchiveId()),
						socket.getAddress(),
						socket.getPort(),
						chunk.index,
						Util.bytesToHex(chunk.tag.getBytes(), 8));
				lastStream.write(ByteBuffer.allocate(4).putInt(chunk.index).array());
				lastStream.write(chunk.getData());
				
				boolean hasMore = queue.expectTagNext(chunk.tag.getBytes());
				if(!hasMore) {
					lastStream.eof();
					lastStream = null;
				}
			} catch(SwarmTimeoutException exc) {
				logger.info("Swarm {} {}:{}: PeerConnection page queue thread skipping chunk {} since it is not available locally",
						Util.formatArchiveId(socket.swarm.config.getArchiveId()),
						socket.getAddress(),
						socket.getPort(),
						Util.bytesToHex(lastTag, 8));
			} catch(SocketClosedException exc) {
			} catch(Exception exc) {
				logger.error("Swarm {} {}:{}: PeerConnection page queue thread caught exception in PeerConnection",
						Util.formatArchiveId(socket.swarm.config.getArchiveId()),
						socket.getAddress(),
						socket.getPort(),
						exc);
				if(exc.getCause() != null) {
					logger.error("Swarm {} {}:{}: PeerConnection page queue thread caught exception in PeerConnection with underlying cause",
							Util.formatArchiveId(socket.swarm.config.getArchiveId()),
							socket.getAddress(),
							socket.getPort(),
							exc.getCause());
				}
				try { Thread.sleep(500); } catch(InterruptedException exc2) {}
			}
		}
		
		if(lastStream != null) {
			lastStream.eof();
		}
	}
	
	protected CryptoSupport getCrypto() {
		return socket.swarm.config.getAccessor().getMaster().getCrypto();
	}

	public boolean isLocalPaused() {
		return localPaused;
	}

	public boolean isRemotePaused() {
		return remotePaused;
	}
	
	public boolean wantsEverything() {
		return wantsEverything;
	}
	
	public long getTimeStart() {
		return timeStart;
	}
	
	public synchronized ArrayList<Long> announcedTags() {
		return new ArrayList<>(announcedTags);
	}

	public boolean retryOnClose() {
		return retryOnClose;
	}

	public void setRetryOnClose(boolean retryOnClose) {
		this.retryOnClose = retryOnClose;
	}
}
