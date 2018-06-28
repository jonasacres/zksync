package com.acrescrypto.zksync.net;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.BlacklistedException;
import com.acrescrypto.zksync.exceptions.InvalidSignatureException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.exceptions.UnconnectableAdvertisementException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.zkfs.ObfuscatedRefTag;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.net.PageQueue.ChunkReference;
import com.acrescrypto.zksync.utility.AppendableInputStream;
import com.acrescrypto.zksync.utility.Util;

public class PeerConnection {
	public final static byte CMD_ACCESS_PROOF = 0x00;
	public final static byte CMD_ANNOUNCE_PEERS = 0x01;
	public final static byte CMD_ANNOUNCE_SELF_AD = 0x02;
	public final static byte CMD_ANNOUNCE_TAGS = 0x03;
	public final static byte CMD_ANNOUNCE_TIPS = 0x04;
	public final static byte CMD_REQUEST_ALL = 0x05;
	public final static byte CMD_REQUEST_ALL_CANCEL = 0x06;
	public final static byte CMD_REQUEST_INODES = 0x07;
	public final static byte CMD_REQUEST_REVISION_CONTENTS = 0x08;
	public final static byte CMD_REQUEST_PAGE_TAGS = 0x09;
	public final static byte CMD_SEND_PAGE = 0x0a;
	public final static byte CMD_SET_PAUSED = 0x0b;
	public final static byte CMD_REQUEST_CONFIG_INFO = 0x0c;
	public final static byte CMD_SEND_CONFIG_INFO = 0x0d;
	
	public final static int MAX_SUPPORTED_CMD = CMD_SEND_CONFIG_INFO; // update to largest acceptable command code
	
	public final static int PEER_TYPE_STATIC = 0; // static fileserver; needs subclass to handle
	public final static int PEER_TYPE_BLIND = 1; // has knowledge of seed key, but not archive passphrase; can't decipher data
	public final static int PEER_TYPE_FULL = 2; // live peer with knowledge of archive passphrase
	
	/** A message can't be sent to the remote peer because this channel hasn't established that we have full read
	 * access to the archive. */
	public class PeerCapabilityException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	protected PeerSocket socket;
	protected HashSet<Long> announcedTags = new HashSet<Long>();
	protected boolean remotePaused, localPaused;
	protected PageQueue queue;
	protected boolean receivedTags, closed;
	protected final Logger logger = LoggerFactory.getLogger(PeerConnection.class);
	boolean sentProof, wantsEverything;
	
	public PeerConnection(PeerSwarm swarm, PeerAdvertisement ad) throws UnsupportedProtocolException, IOException, ProtocolViolationException, BlacklistedException {
		this.socket = PeerSocket.connectToAd(swarm, ad);
		this.socket.handshake(this);
		initialize();
	}
	
	public PeerConnection(PeerSocket socket) throws IOException {
		this.socket = socket;
		initialize();
	}
	
	protected void initialize() throws IOException {
		socket.connection = this;
		this.queue = new PageQueue(socket.swarm.config);
		new Thread(()->pageQueueThread()).start();
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

	public synchronized void close() {
		if(closed) return;
		closed = true;
		
		if(queue != null) {
			queue.close();
		}
		
		try {
			if(socket != null) {
				socket.close();
			}
		} catch(IOException exc) {
			logger.warn("Caught exception closing socket to address {}", socket.getAddress(), exc);
		} finally {
			if(socket != null && socket.swarm != null) {
				socket.swarm.closedConnection(this);
			}
			
			synchronized(this) {
				this.notifyAll();
			}
		}
	}

	public void announceTag(long shortTag) {
		ByteBuffer tag = ByteBuffer.allocate(RefTag.REFTAG_SHORT_SIZE);
		tag.putLong(shortTag);
		send(CMD_ANNOUNCE_TAGS, tag.array());
		if(wantsEverything) {
			// TODO DHT: (test) Test that page tags are automatically queued if everything requested (and not queued if everything not requested)
			queue.addPageTag(PageQueue.DEFAULT_EVERYTHING_PRIORITY, shortTag);
		}
	}
	
	public void announceSelf(PeerAdvertisement ad) {
		byte[] serializedAd = ad.serialize();
		ByteBuffer serialized = ByteBuffer.allocate(2+serializedAd.length);
		assert(serializedAd.length <= Short.MAX_VALUE);
		serialized.putShort((short) serializedAd.length);
		serialized.put(serializedAd);
		send(CMD_ANNOUNCE_SELF_AD, serialized.array());
	}
	
	public void announcePeer(PeerAdvertisement ad) {
		byte[] serializedAd = ad.serialize();
		ByteBuffer serialized = ByteBuffer.allocate(2+serializedAd.length);
		assert(serializedAd.length <= Short.MAX_VALUE);
		serialized.putShort((short) serializedAd.length);
		serialized.put(serializedAd);
		send(CMD_ANNOUNCE_PEERS, serialized.array());
	}
	
	public void announcePeers(Collection<PeerAdvertisement> ads) {
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
		ByteBuffer serialized = ByteBuffer.allocate(tags.size() * RefTag.REFTAG_SHORT_SIZE);
		for(Long shortTag : tags) {
			serialized.putLong(shortTag);
		}
		send(CMD_ANNOUNCE_TAGS, serialized.array());
	}
	
	public void announceTags(Collection<byte[]> tags) {
		ByteBuffer serialized = ByteBuffer.allocate(tags.size() * RefTag.REFTAG_SHORT_SIZE);
		for(byte[] tag : tags) {
			serialized.putLong(Util.shortTag(tag));
		}
		send(CMD_ANNOUNCE_TAGS, serialized.array());
	}
	
	public void announceTags() {
		ZKArchive archive = socket.swarm.config.getArchive();
		if(archive == null) {
			send(CMD_ANNOUNCE_TAGS, new byte[0]);
			return;
		}
		
		announceTags(archive.allPageTags());
	}
	
	public void announceTips() throws IOException {
		ZKArchive archive = socket.swarm.config.getArchive();
		if(archive == null || archive.getConfig().getRevisionTree() == null) {
			send(CMD_ANNOUNCE_TIPS, new byte[0]);
			return;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(archive.getConfig().getRevisionTree().branchTips().size() * ObfuscatedRefTag.sizeForConfig(socket.swarm.config));
		for(RefTag tag : archive.getConfig().getRevisionTree().plainBranchTips()) {
			buf.put(tag.obfuscate().serialize());
		}
		
		send(CMD_ANNOUNCE_TIPS, buf.array());
	}
	
	public void requestAll() {
		send(CMD_REQUEST_ALL, new byte[0]);
	}
	
	public void requestAllCancel() {
		send(CMD_REQUEST_ALL_CANCEL, new byte[0]);
	}
	
	public void requestPageTag(int priority, long shortTag) {
		ByteBuffer buf = ByteBuffer.allocate(4+RefTag.REFTAG_SHORT_SIZE);
		buf.putInt(priority);
		buf.putLong(shortTag);
		send(CMD_REQUEST_PAGE_TAGS, buf.array());
	}

	public void requestPageTags(int priority, Collection<Long> pageTags) {
		if(pageTags.isEmpty()) return;
		ByteBuffer pageTagsMerged = ByteBuffer.allocate(4+RefTag.REFTAG_SHORT_SIZE*pageTags.size());
		pageTagsMerged.putInt(priority);
		for(Long shortTag : pageTags) {
			pageTagsMerged.putLong(shortTag);
		}
		
		send(CMD_REQUEST_PAGE_TAGS, pageTagsMerged.array());
	}
	
	/** Request encrypted files pertaining to a given inode (including page tree chunks). */
	public void requestInodes(int priority, RefTag revTag, Collection<Long> inodeIds) throws PeerCapabilityException {
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
	
	public void requestRevisionContents(int priority, Collection<RefTag> tips) throws PeerCapabilityException {
		if(tips.isEmpty()) return;
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_CONTENTS, serializeRefTags(priority, tips));
	}
	
	public void requestConfigInfo() {
		send(CMD_REQUEST_CONFIG_INFO, new byte[0]);
	}
	
	public void sendConfigInfo() {
		ByteBuffer configInfo = ByteBuffer.allocate(8+socket.swarm.config.getCrypto().asymPublicDHKeySize() + socket.swarm.config.getCrypto().hashLength());
		configInfo.put(socket.swarm.config.getArchiveFingerprint());
		configInfo.put(socket.swarm.config.getPubKey().getBytes());
		configInfo.putLong(socket.swarm.config.getPageSize());
		send(CMD_SEND_CONFIG_INFO, configInfo.array());
	}
	
	public void setPaused(boolean paused) {
		send(CMD_SET_PAUSED, new byte[] { (byte) (paused ? 0x01 : 0x00) });
	}
	
	public synchronized void setLocalPaused(boolean localPaused) {
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
	
	public synchronized void waitForReady() throws SocketClosedException {
		while(!receivedTags) {
			try {
				this.wait();
			} catch (InterruptedException e) {}
			assertConnected();
		}
	}

	public boolean handle(PeerMessageIncoming msg) throws ProtocolViolationException {
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
			case CMD_REQUEST_PAGE_TAGS:
				handleRequestPageTags(msg);
				break;
			case CMD_SEND_PAGE:
				handleSendPage(msg);
				break;
			case CMD_SET_PAUSED:
				handleSetPaused(msg);
				break;
			case CMD_REQUEST_CONFIG_INFO:
				handleRequestConfigInfo(msg);
				break;
			case CMD_SEND_CONFIG_INFO:
				handleSendConfigInfo(msg);
				break;
			default:
				logger.info("Ignoring unknown request command {} from {}", msg.cmd, socket.getAddress());
				return false;
			}
		} catch(EOFException exc) {
			// ignore these
		} catch(PeerCapabilityException | IOException | InvalidSignatureException exc) {
			/* Arguably, blacklisting people because we had a local IOException is unfair. But, there are two real
			 * possibilities here:
			 *   1. They're doing something nasty that's triggering IOExceptions. THey deserve it.
			 *   2. They're not doing anything nasty. In which case, we're getting IOExceptions doing normal stuff, so
			 *      it's not like we can participate as peers right now anyway.
			 * Sadly, there's the possibility that we cut ourselves off for a long time because we had a volume go
			 * unmounted or something. That's not great. An opportunity for future improvement...
			 */
			logger.warn("Blacklisting peer " + socket.getAddress() + " due to exception", exc);
			throw new ProtocolViolationException();
		}
		
		return true;
	}

	protected void handleAnnouncePeers(PeerMessageIncoming msg) throws EOFException, ProtocolViolationException {
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
		int adLen = Util.unsignShort(msg.rxBuf.getShort());
		assertState(0 < adLen && adLen <= PeerMessage.MESSAGE_SIZE);
		byte[] adRaw = new byte[adLen];
		msg.rxBuf.get(adRaw);
		try {
			PeerAdvertisement ad = PeerAdvertisement.deserializeRecordWithAddress(msg.connection.getCrypto(), ByteBuffer.wrap(adRaw), msg.connection.socket.getAddress(), msg.connection.socket.getPort());
			if(ad != null && !ad.isBlacklisted(socket.swarm.config.getAccessor().getMaster().getBlacklist())) {
				socket.swarm.addPeerAdvertisement(ad);
			}
		} catch (IOException | UnconnectableAdvertisementException e) {
		}
		
		msg.rxBuf.requireEOF();
	}
	
	protected void handleAnnounceTags(PeerMessageIncoming msg) throws EOFException {
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
			receivedTags = true;
			this.notifyAll();
		}
	}
	
	protected void handleAnnounceTips(PeerMessageIncoming msg) throws InvalidSignatureException, IOException {
		Util.blockOn(()->!socket.swarm.config.isInitialized() && !closed);
		
		byte[] obfTagRaw = new byte[ObfuscatedRefTag.sizeForConfig(socket.swarm.config)];
		while(msg.rxBuf.hasRemaining()) {
			msg.rxBuf.get(obfTagRaw);
			ObfuscatedRefTag obfTag = new ObfuscatedRefTag(socket.swarm.config, obfTagRaw);
			socket.swarm.config.getRevisionTree().addBranchTip(obfTag);
		}
		
		socket.swarm.config.getRevisionTree().write();
	}
	
	protected void handleRequestAll(PeerMessageIncoming msg) throws ProtocolViolationException, IOException {
		msg.rxBuf.requireEOF();
		sendEverything();
	}

	protected void handleRequestAllCancel(PeerMessageIncoming msg) throws ProtocolViolationException, IOException {
		msg.rxBuf.requireEOF();
		stopSendingEverything();
	}

	protected void handleRequestInodes(PeerMessageIncoming msg) throws PeerCapabilityException, IOException {
		ZKArchive archive = socket.swarm.config.getArchive();
		assertPeerCapability(PEER_TYPE_FULL);
		byte[] refTagBytes = new byte[archive.getConfig().refTagSize()];
		int priority = msg.rxBuf.getInt();
		RefTag tag = new RefTag(archive, msg.rxBuf.read(refTagBytes));
		
		while(msg.rxBuf.hasRemaining()) {
			long inodeId = msg.rxBuf.getLong();
			sendInodeContents(priority, tag, inodeId);
		}
	}
	
	protected void handleRequestRevisionContents(PeerMessageIncoming msg) throws PeerCapabilityException, IOException {
		ZKArchive archive = socket.swarm.config.getArchive();
		assertPeerCapability(PEER_TYPE_FULL);
		byte[] refTagBytes = new byte[archive.getConfig().refTagSize()];
		int priority = msg.rxBuf.getInt();
		
		while(msg.rxBuf.hasRemaining()) {
			RefTag tag = new RefTag(archive, msg.rxBuf.read(refTagBytes));
			sendRevisionContents(priority, tag);
		}
	}
	
	protected void handleRequestPageTags(PeerMessageIncoming msg) throws IOException {
		byte[] shortTag = new byte[RefTag.REFTAG_SHORT_SIZE];
		int priority = msg.rxBuf.getInt();
		while(msg.rxBuf.hasRemaining()) {
			msg.rxBuf.get(shortTag);
			sendPageTag(priority, ByteBuffer.wrap(shortTag).getLong());
		}
	}
	
	protected void handleSendPage(PeerMessageIncoming msg) throws IOException, ProtocolViolationException {
		byte[] tag = msg.rxBuf.read(socket.swarm.config.getCrypto().hashLength());
		if(socket.swarm.getConfig().getCacheStorage().exists(Page.pathForTag(tag))) {
			announceTag(Util.shortTag(tag));
			return;
		}
		
		int actualPageSize = socket.swarm.config.getSerializedPageSize();
		int expectedChunks = (int) Math.ceil(((double) actualPageSize)/PeerMessage.FILE_CHUNK_SIZE);
		int finalChunkSize = actualPageSize % PeerMessage.FILE_CHUNK_SIZE;
		ChunkAccumulator accumulator = socket.swarm.accumulatorForTag(tag);
		
		while(!accumulator.isFinished() && msg.rxBuf.hasRemaining()) {
			long offset = Util.unsignInt(msg.rxBuf.getInt());
			assertState(0 <= offset && offset < expectedChunks && offset <= Integer.MAX_VALUE);
			int readLen = offset == expectedChunks - 1 ? finalChunkSize : PeerMessage.FILE_CHUNK_SIZE;
			byte[] chunkData = msg.rxBuf.read(readLen);
			accumulator.addChunk((int) offset, chunkData, this);
		}
	}
	
	protected void handleSetPaused(PeerMessageIncoming msg) throws ProtocolViolationException, EOFException {
		byte pausedByte = msg.rxBuf.get();
		assertState(pausedByte == 0x00 || pausedByte == 0x01);
		msg.rxBuf.requireEOF();
		setRemotePaused(pausedByte == 0x01);
	}
	
	protected void handleRequestConfigInfo(PeerMessageIncoming msg) throws ProtocolViolationException {
		msg.rxBuf.requireEOF();
		if(socket.swarm.config.canReceive()) {
			sendConfigInfo();
		}
	}
	
	protected void handleSendConfigInfo(PeerMessageIncoming msg) throws EOFException, ProtocolViolationException {
		byte[] fingerprint = new byte[getCrypto().hashLength()];
		byte[] pubKeyRaw = new byte[getCrypto().asymPublicDHKeySize()];
		
		msg.rxBuf.get(fingerprint);
		msg.rxBuf.get(pubKeyRaw);
		long pageSizeLong = msg.rxBuf.getLong();
		msg.rxBuf.requireEOF();
		
		assertState(0 <= pageSizeLong && pageSizeLong <= Integer.MAX_VALUE);
		byte[] allegedArchiveId = socket.swarm.config.calculateArchiveId(fingerprint, pubKeyRaw, pageSizeLong);
		assertState(Arrays.equals(socket.swarm.config.getArchiveId(), allegedArchiveId));
		
		if(!socket.swarm.config.canReceive()) {
			socket.swarm.config.setPageSize((int) pageSizeLong);
			socket.swarm.config.setArchiveFingerprint(fingerprint);
			socket.swarm.config.setPubKey(getCrypto().makePublicSigningKey(pubKeyRaw));
		}
		
		socket.swarm.receivedConfigInfo();
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

	protected void sendRevisionContents(int priority, RefTag refTag) throws IOException {
		queue.addRevisionTag(priority, refTag);
	}
	
	protected void sendInodeContents(int priority, RefTag revTag, long inodeId) throws IOException {
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

	protected byte[] serializeRefTags(int priority, Collection<RefTag> tags) {
		ByteBuffer buf = ByteBuffer.allocate(4 + tags.size() * socket.swarm.config.refTagSize());
		buf.putInt(priority);
		for(RefTag tag : tags) buf.put(tag.getBytes());
		return buf.array();
	}

	protected void assertPeerCapability(int capability) throws PeerCapabilityException {
		if(socket.getPeerType() < capability) throw new PeerCapabilityException();
	}

	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}

	protected void pageQueueThread() {
		Thread.currentThread().setName("PeerConnection queue thread");
		byte[] lastTag = new byte[0];
		AppendableInputStream lastStream = null;
		
		while(!socket.isClosed()) {
			try {
				ChunkReference chunk = queue.nextChunk();
				if(chunk == null || !wantsFile(chunk.tag)) continue;
				waitForUnpause();
				
				if(lastStream == null || !Arrays.equals(lastTag, chunk.tag)) {
					if(lastStream != null) {
						lastStream.eof();
					}
					
					lastStream = new AppendableInputStream();
					socket.makeOutgoingMessage(CMD_SEND_PAGE, lastStream);
					lastStream.write(chunk.tag);
					lastTag = chunk.tag;
				}

				
				lastStream.write(ByteBuffer.allocate(4).putInt(chunk.index).array());
				lastStream.write(chunk.getData());
				if(!queue.expectTagNext(chunk.tag)) {
					lastStream.eof();
					lastStream = null;
				}
			} catch(Exception exc) {
				logger.error("Caught exception in PeerConnection page queue thread", exc);
				try { Thread.sleep(500); } catch(InterruptedException exc2) {}
			}
		}
	}
	
	protected CryptoSupport getCrypto() {
		return socket.swarm.config.getAccessor().getMaster().getCrypto();
	}
}
