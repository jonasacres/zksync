package com.acrescrypto.zksync.net;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.utility.Util;

public class PeerConnection {
	public final static boolean DISABLE_TAG_LIST = false;
	
	public final static byte CMD_ACCESS_PROOF               = 0x00;
	public final static byte CMD_ANNOUNCE_PEERS             = 0x01;
	public final static byte CMD_ANNOUNCE_SELF_AD           = 0x02;
	public final static byte CMD_ANNOUNCE_TAGS              = 0x03;
	public final static byte CMD_ANNOUNCE_TIPS              = 0x04;
	public final static byte CMD_ANNOUNCE_REVISION_DETAILS  = 0x05;
	public final static byte CMD_REQUEST_ALL                = 0x06;
	public final static byte CMD_REQUEST_ALL_CANCEL         = 0x07;
	public final static byte CMD_REQUEST_INODES             = 0x08;
	public final static byte CMD_REQUEST_REVISION_CONTENTS  = 0x09;
	public final static byte CMD_REQUEST_REVISION_STRUCTURE = 0x0a;
	public final static byte CMD_REQUEST_REVISION_DETAILS   = 0x0b;
	public final static byte CMD_REQUEST_PAGE_TAGS          = 0x0c;
	public final static byte CMD_SEND_PAGE                  = 0x0d;
	public final static byte CMD_SET_PAUSED                 = 0x0e;
	
	public final static int MAX_SUPPORTED_CMD = CMD_SET_PAUSED; // update to largest acceptable command code
	
	public final static int PEER_TYPE_STATIC = 0; // static fileserver; needs subclass to handle
	public final static int PEER_TYPE_BLIND  = 1; // has knowledge of seed key, but not archive passphrase; can't decipher data
	public final static int PEER_TYPE_FULL   = 2; // live peer with knowledge of archive passphrase
	
	/** A message can't be sent to the remote peer because this channel hasn't established that we have full read
	 * access to the archive. */
	public class PeerCapabilityException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	public interface PeerConnectionUnpauseCallback {
		void unpaused() throws IOException;
	}
	
	public interface PeerConnectionFullInitCallback {
		void initialized() throws IOException, ProtocolViolationException, PeerCapabilityException;
	}
	
	protected PeerSocket                                  socket;
	protected PageQueue                                   queue;
	protected long                                        timeStart;
	protected boolean                                     wantsEverything,
	                                                      retryOnClose,
	                                                      closed,
	                                                      remotePaused,
	                                                      localPaused,
	                                                      isFullyInitialized;
	protected PeerMessageOutgoing                         lastPageQueueMsg;
	protected StorageTag                                  lastTagFromQueue;
	protected HashSet    <Long>                           announcedTags        = new HashSet<>(); // TODO Someday: (review) unbounded memory use to track announced page tags
	protected LinkedList <PeerConnectionUnpauseCallback>  unpauseCallbacks     = new LinkedList<>();
	protected LinkedList <PeerConnectionFullInitCallback> fullInitCallbacks    = new LinkedList<>();
	
	protected Logger              logger = LoggerFactory.getLogger(PeerConnection.class);
	
	public PeerConnection(PeerSwarm swarm, PeerAdvertisement ad) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		this.socket = PeerSocket.connectToAd(swarm, ad);
		this.socket.handshake(this, ()->{
			initialize();
			announceSelf();
		});
	}
	
	public PeerConnection(PeerSocket socket) throws IOException {
		this.socket = socket;
		initialize();
	}
	
	protected void initialize() throws IOException {
		this.retryOnClose = true;
		socket.connection = this;
		timeStart         = Util.currentTimeMillis();
		this.queue        = new PageQueue(socket.swarm.config);
		
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
	 * @throws IOException 
	 */
	public void announceTag(long shortTag) throws IOException {
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
	
	public void announceSelf() throws IOException {
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
	
	public void announceSelf(PeerAdvertisement ad) throws IOException {
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
	
	public void announcePeer(PeerAdvertisement ad) throws IOException {
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
	
	public void announcePeers(Collection<PeerAdvertisement> ads) throws IOException {
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
	
	public void announceShortTags(Collection<Long> tags) throws IOException {
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
	
	public void announceTags(Collection<StorageTag> tags) throws IOException {
		if(DISABLE_TAG_LIST) return;
		logger.trace("Swarm {} {}:{}: PeerConnection send announceTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		ByteBuffer serialized = ByteBuffer.allocate(tags.size() * RefTag.REFTAG_SHORT_SIZE);
		for(StorageTag tag : tags) {
			if(!serialized.hasRemaining()) break; // possible to add tags as we iterate
			serialized.putLong(tag.shortTagPreserialized());
		}
		send(CMD_ANNOUNCE_TAGS, serialized.array());
	}
	
	public void announceTags() throws IOException {
		if(DISABLE_TAG_LIST) return;
		ZKArchive archive = socket.swarm.config.getArchive();
		if(archive == null) {
			send(CMD_ANNOUNCE_TAGS, new byte[0]);
			return;
		}
		
		announceTags(new ArrayList<>(archive.allPageTags()));
	}
	
	public void announceTip(RevisionTag tip) throws IOException {
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
	
	public void announceRevisionDetails(RevisionTag tag) throws IOException {
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
	
	public void requestAll() throws IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestAll",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		send(CMD_REQUEST_ALL, new byte[0]);
	}
	
	public void requestAllCancel() throws IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestAllCancel",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		send(CMD_REQUEST_ALL_CANCEL, new byte[0]);
	}
	
	public void requestPageTag(int priority, long shortTag) throws IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestPageTag",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		ByteBuffer buf = ByteBuffer.allocate(4+RefTag.REFTAG_SHORT_SIZE);
		buf.putInt(priority);
		buf.putLong(shortTag);
		send(CMD_REQUEST_PAGE_TAGS, buf.array());
	}

	public void requestPageTags(int priority, Collection<Long> pageTags) throws IOException {
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
	
	/** Request encrypted files pertaining to a given inode (including page tree chunks). 
	 * @throws IOException */
	public void requestInodes(int priority, RevisionTag revTag, Collection<Long> inodeIds) throws PeerCapabilityException, IOException {
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
	
	public void requestRevisionContents(int priority, Collection<RevisionTag> tips) throws PeerCapabilityException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestRevisionContents",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(tips.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_CONTENTS, serializeRevTags(priority, tips));
	}

	public void requestRevisionStructure(int priority, Collection<RevisionTag> tips) throws PeerCapabilityException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestRevisionStructure",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(tips.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_STRUCTURE, serializeRevTags(priority, tips));
	}

	public void requestRevisionDetails(int priority, RevisionTag revTag) throws PeerCapabilityException, IOException {
		ArrayList<RevisionTag> list = new ArrayList<>();
		list.add(revTag);
		requestRevisionDetails(priority, list);
	}
	
	public void requestRevisionDetails(int priority, Collection<RevisionTag> revTags) throws PeerCapabilityException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection send requestRevisionDetails",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		if(revTags.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_DETAILS, serializeRevTags(priority, revTags));
	}
	
	public void setPaused(boolean paused) throws IOException {
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

	public boolean wantsFile(StorageTag tag) {
		return !announcedTags.contains(tag.shortTagPreserialized());
	}

	public boolean hasFile(long shortTag) {
		return announcedTags.contains(shortTag);
	}
	
	public void checkInitialization() throws IOException, ProtocolViolationException, PeerCapabilityException {
		if(isFullyInitialized) return;
		
		boolean haveKey          = socket.swarm.config.getAccessor().isSeedOnly()
				                || socket.swarm.config.hasKey();
		boolean haveArchive      = socket.swarm.config.getArchive() == null;
		this.isFullyInitialized  = haveKey && haveArchive;
		
		for(PeerConnectionFullInitCallback callback : fullInitCallbacks) {
			callback.initialized();
		}
	}
	
	public void onFullInit(PeerConnectionFullInitCallback callback) throws IOException, ProtocolViolationException, PeerCapabilityException {
		if(isFullyInitialized) {
			callback.initialized();
		} else {
			fullInitCallbacks.add(callback);
		}
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
		msg.keepExpecting(2, (adLenBuf)->{
			int adLen = Util.unsignShort(adLenBuf.getShort());
			assertState(    0 <  adLen                   );
			assertState(adLen <= PeerMessage.MESSAGE_SIZE);
			
			msg.expect(adLen, (adBuf)->{
				CryptoSupport crypto = socket.swarm.config.getAccessor().getMaster().getCrypto();
				try {
					PeerAdvertisement ad = PeerAdvertisement.deserializeRecord(crypto, adBuf);
					if(ad == null) return;
					if(ad.isBlacklisted(socket.swarm.config.getAccessor().getMaster().getBlacklist())) return;
					socket.swarm.addPeerAdvertisement(ad);
				} catch (IOException | UnconnectableAdvertisementException e) {
				}
			});
		});
	}
	
	protected void handleAnnounceSelfAd(PeerMessageIncoming msg) throws ProtocolViolationException, EOFException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceSelfAd",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		msg.expect(2, (adLenBuf)->{
			int adLen = Util.unsignShort(adLenBuf.getShort());
			assertState(    0 <  adLen                   );
			assertState(adLen <= PeerMessage.MESSAGE_SIZE);
			
			msg.expect(adLen, (adBuf)->{
				Blacklist blacklist = socket.swarm.config.getAccessor().getMaster().getBlacklist();
				try {
					PeerAdvertisement ad = PeerAdvertisement.deserializeRecordWithAddress(
							msg.connection.getCrypto(),
							adBuf,
							msg.connection.socket.getAddress()
						);
					if(ad != null && !ad.isBlacklisted(blacklist)) {
						msg.requireFinish(()->{
							socket.swarm.addPeerAdvertisement(ad);
						});
					}
				} catch (IOException | UnconnectableAdvertisementException e) {
				}
			});
		});
	}
	
	protected void handleAnnounceTags(PeerMessageIncoming msg) throws EOFException {
		if(DISABLE_TAG_LIST) return;
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		ByteBuffer surplus = ByteBuffer.allocate(RefTag.REFTAG_SHORT_SIZE);
		assert(RefTag.REFTAG_SHORT_SIZE == 8); // This code depends on tags being sent as 64-bit values.
		
		msg.keepExpecting(1, (buf)->{
			if(surplus.position() > 0) {
				surplus.put(buf);
				if(!surplus.hasRemaining()) {
					long shortTag = surplus.getLong(); // 8 bytes == REFTAG_SHORT_SIZE
					announcedTags.add(shortTag);
					surplus.clear();
				}
			}
			
			while(buf.remaining() >= RefTag.REFTAG_SHORT_SIZE) {
				long shortTag = buf.getLong(); // 8 bytes == REFTAG_SHORT_SIZE
				announcedTags.add(shortTag);
			}
			
			surplus.put(buf);
			// TODO: Notify that we've acquired new tags (used to be a this.notifyAll)
		});
	}
	
	protected void handleAnnounceTips(PeerMessageIncoming msg) throws InvalidSignatureException, IOException, ProtocolViolationException, PeerCapabilityException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceTips",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			int revTagSize = RevisionTag.sizeForConfig(socket.swarm.config);
			byte[] revTagBytes = new byte[revTagSize];
			
			msg.keepExpecting(revTagSize, (revTagBuf)->{
				revTagBuf.get(revTagBytes);
				RevisionTag revTag = new RevisionTag(socket.swarm.config, revTagBytes, false);
				
				// TODO: this is a blocking operation, needs to become async (caller does not care about result here)
				socket.swarm.config.getRevisionList().addBranchTip(revTag, true);
			});
			
			msg.onFinish(()->{
				socket.swarm.config.getRevisionList().write();
			});
		});
	}
	
	protected void handleAnnounceRevisionDetails(PeerMessageIncoming msg) throws PeerCapabilityException, IOException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv announceRevisionDetails",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			assertPeerCapability(PEER_TYPE_FULL);

			int                  revTagSize = RevisionTag.sizeForConfig(socket.swarm.config);
			byte[]              revTagBytes = new byte[revTagSize],
			                    parentBytes = new byte[revTagSize];
			LinkedList<RevisionTag> parents = new LinkedList<>();
			
			msg.expect(revTagBytes.length, (revTagBuf)->{
				revTagBuf.get(revTagBytes);
			});
			
			msg.keepExpecting(revTagBytes.length, (parentTagBuf)->{
				parentTagBuf.get(parentBytes);
				parents.add(new RevisionTag(
						socket.getSwarm().getConfig(),
						parentBytes,
						true
					));
			});
			
			msg.onFinish(()->{
				ZKArchive archive = socket.swarm.config.getArchive();
				RevisionTag revTag = new RevisionTag(archive.getConfig(), revTagBytes, true);
				socket.swarm.config.getRevisionTree().addParentsForTag(revTag, parents);
			});
		});
	}
	
	protected void handleRequestAll(PeerMessageIncoming msg) throws ProtocolViolationException, IOException, PeerCapabilityException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestAll",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			msg.requireFinish(()->{
				sendEverything();
			});
		});
	}

	protected void handleRequestAllCancel(PeerMessageIncoming msg) throws ProtocolViolationException, IOException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestAllCancel",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		msg.requireFinish(()->{
			stopSendingEverything();
		});
	}

	protected void handleRequestInodes(PeerMessageIncoming msg) throws PeerCapabilityException, IOException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestInodes",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			ZKArchive archive = socket.swarm.config.getArchive();
			
			assertPeerCapability(PEER_TYPE_FULL);
			
			int revTagSize = RevisionTag.sizeForConfig(archive.getConfig());
			msg.expect(4 + revTagSize, (buf)->{
				int priority       = buf.getInt();
				byte[] revTagBytes = new byte[revTagSize];
				buf.get(revTagBytes);
				
				RevisionTag tag = new RevisionTag(archive.getConfig(), revTagBytes, false);
				
				msg.keepExpecting(8, (inodeIdBuf)->{
					long inodeId = inodeIdBuf.getLong();
					sendInodeContents(priority, tag, inodeId);
				});
			});
		});
	}
	
	protected void handleRequestRevisionContents(PeerMessageIncoming msg) throws PeerCapabilityException, IOException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestRevisionContents",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			ZKArchive archive = socket.swarm.config.getArchive();
	
			assertPeerCapability(PEER_TYPE_FULL);
			msg.expect(4, (priorityBuf)->{
				int priority   = priorityBuf.getInt();
				int revTagSize = RevisionTag.sizeForConfig(archive.getConfig());
				
				msg.keepExpecting(revTagSize, (revTagBuf)->{
					byte[] revTagBytes = new byte[revTagSize];
					revTagBuf.get(revTagBytes);
					RevisionTag tag = new RevisionTag(
							archive.getConfig(),
							revTagBytes,
							false);
					sendRevisionContents(priority, tag);
				});
			});
		});
	}
	
	protected void handleRequestRevisionStructure(PeerMessageIncoming msg) throws PeerCapabilityException, IOException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestRevisionStructure",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			ZKArchive archive = socket.swarm.config.getArchive();

			assertPeerCapability(PEER_TYPE_FULL);
			msg.expect(4, (priorityBuf)->{
				int priority = priorityBuf.getInt();
				byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(archive.getConfig())];
				
				msg.keepExpecting(revTagBytes.length, (revTagBuf)->{
					revTagBuf.get(revTagBytes);
					RevisionTag tag = new RevisionTag(archive.getConfig(), revTagBytes, false);
					sendRevisionStructure(priority, tag);
				});
			});
		});
	}
	
	protected void handleRequestRevisionDetails(PeerMessageIncoming msg) throws PeerCapabilityException, IOException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestRevisionDetails",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			ZKArchive archive = socket.swarm.config.getArchive();

			assertPeerCapability(PEER_TYPE_FULL);
			byte[] revTagBytes = new byte[RevisionTag.sizeForConfig(archive.getConfig())];
			
			// TODO Someday: (implement) honor prioritization requests for revision details, or drop priority field altogether
			msg.expect(4, (priorityBuf)->{}); // 4 byte priority is ignored for now
			msg.keepExpecting(revTagBytes.length, (revTagBuf)->{
				revTagBuf.get(revTagBytes);
				RevisionTag tag = new RevisionTag(
						archive.getConfig(),
						revTagBytes,
						false
					);
				announceRevisionDetails(tag);
			});
		});
	}
	
	protected void handleRequestPageTags(PeerMessageIncoming msg) throws IOException, ProtocolViolationException, PeerCapabilityException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv requestPageTags",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		onFullInit(()->{
			msg.expect(4, (priorityBuf)->{
				int priority = priorityBuf.getInt();
				msg.keepExpecting(RefTag.REFTAG_SHORT_SIZE, (shortTagBuf)->{
					long shortTag = shortTagBuf.getLong();
					sendPageTag(priority, shortTag);
				});
			});
		});
	}
	
	protected void handleSendPage(PeerMessageIncoming msg) throws IOException, ProtocolViolationException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv sendPage {}",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort(),
				msg.msgId);
		int storageTagLength = socket.swarm.config.getCrypto().hashLength(); 
		msg.expect(storageTagLength, (storageTagBuf)->{
			byte[] tagBytes = new byte[storageTagLength];
			storageTagBuf.get(tagBytes);
			StorageTag tag = new StorageTag(socket.swarm.config.getCrypto(), tagBytes);
			
			boolean isConfigTag = tag.equals(socket.swarm.getConfig().tag());
			
			if(isConfigTag) {
				finishHandlingSendPage(msg, tag);
			} else {
				onFullInit( () -> finishHandlingSendPage(msg, tag) );
			}
		});
	}
	
	protected void finishHandlingSendPage(PeerMessageIncoming msg, StorageTag tag) throws IOException, ProtocolViolationException {
		if(socket.swarm.getConfig().getCacheStorage().exists(tag.path())) {
			logger.trace("Swarm {} {}:{}: PeerConnection handleSendPage {} ignoring offered page {} (already have it)",
					Util.formatArchiveId(socket.swarm.config.getArchiveId()),
					socket.getAddress(),
					socket.getPort(),
					msg.msgId,
					tag);
			if(!DISABLE_TAG_LIST) {
				announceTag(tag.shortTag());
			}
			
			msg.close();
			return;
		}
		
		logger.trace("Swarm {} {}:{}: PeerConnection handleSendPage {} receiving page {}",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort(),
				msg.msgId,
				tag.shortTag());
		ChunkAccumulator accumulator = socket.swarm.accumulatorForTag(tag);		
		int actualPageSize = socket.swarm.config.getSerializedPageSize();
		int expectedChunks = (int) Math.ceil(((double) actualPageSize)/PeerMessage.FILE_CHUNK_SIZE);
		int finalChunkSize = actualPageSize % PeerMessage.FILE_CHUNK_SIZE;
		
		msg.keepExpecting(4, (offsetBuf)->{
			long offset = Util.unsignInt(offsetBuf.getInt());
			assertState(0      <= offset);
			assertState(offset <  expectedChunks);
			assertState(offset <= Integer.MAX_VALUE);
			int readLen = offset == expectedChunks - 1
					        ? finalChunkSize
					        : PeerMessage.FILE_CHUNK_SIZE;
			
			msg.expect(readLen, (chunkBuf)->{
				byte[] chunkData = new byte[readLen];
				logger.trace("Swarm {} {}:{}: PeerConnection handleSendPage {} received page {} chunk {}",
						Util.formatArchiveId(socket.swarm.config.getArchiveId()),
						socket.getAddress(),
						socket.getPort(),
						msg.msgId,
						tag,
						offset);
				accumulator.addChunk((int) offset, chunkData, this);
				if(accumulator.isFinished()) {
					boolean isConfigTag = tag.equals(socket.swarm.getConfig().tag());
					if(isConfigTag) {
						checkInitialization();
					}
					
					msg.close();
				}
			});
		});
	}
	
	protected void handleSetPaused(PeerMessageIncoming msg) throws ProtocolViolationException, EOFException {
		logger.trace("Swarm {} {}:{}: PeerConnection recv setPaused",
				Util.formatArchiveId(socket.swarm.config.getArchiveId()),
				socket.getAddress(),
				socket.getPort());
		
		msg.expect(1, (pausedBuf)->{
			byte pausedByte = pausedBuf.get();
			assertState(pausedByte == 0x00 || pausedByte == 0x01);
			msg.requireFinish(()->{
				setRemotePaused(pausedByte == 0x01);
			});
		});
	}
	
	protected void send(byte cmd, byte[] payload) throws IOException {
		assert(0 <= cmd && cmd <= Byte.MAX_VALUE);
		socket.makeOutgoingMessage(cmd, ByteBuffer.wrap(payload));
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
	
	public void onUnpause(PeerConnectionUnpauseCallback callback) {
		unpauseCallbacks.add(callback);
	}
	
	protected void sendNextChunk() throws IOException {
		if(socket.isClosed()) return;
		
		queue.nextChunk((chunk)->{
			if(socket.isClosed()) return true;
			if(!wantsFile(chunk.tag)) {
				return false; // tell the queue to invoke us again with the next chunk
			}
			
			if(this.isPaused()) {
				this.onUnpause( ()-> sendNextChunk() );
				return true;
			}
			
			boolean needsNewMsg = !lastTagFromQueue.equals(chunk.tag)
					           ||  lastPageQueueMsg   ==    null
					           ||  lastPageQueueMsg.isFinished();
			if(needsNewMsg) {
				if(lastPageQueueMsg != null) lastPageQueueMsg.finish();
				
				lastPageQueueMsg = socket.makeOutgoingMessage(CMD_SEND_PAGE);
				lastPageQueueMsg.send(ByteBuffer.wrap(chunk.tag.getTagBytes()), true);
				lastTagFromQueue = chunk.tag;
			}
			
			lastPageQueueMsg.send(ByteBuffer.wrap(Util.serializeInt(chunk.index)), true);
			lastPageQueueMsg.send(ByteBuffer.wrap(chunk.getData()), (more)->{
				more.setValue(queue.expectTagNext(lastTagFromQueue));
				sendNextChunk();
				return null;
			});
			
			return true;
		});
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
