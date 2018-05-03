package com.acrescrypto.zksync.net;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.ChunkableFileHandle;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.zkfs.ArchiveAccessor;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.net.PeerSocket.PeerSocketDelegate;

public class PeerConnection implements PeerSocketDelegate {
	public final int CMD_ACCESS_PROOF = 0x00;
	public final int CMD_ANNOUNCE_PEERS = 0x01;
	public final int CMD_ANNOUNCE_TAGS = 0x02;
	public final int CMD_ANNOUNCE_TIPS = 0x03;
	public final int CMD_REQUEST_ALL = 0x04;
	public final int CMD_REQUEST_REF_TAGS = 0x05;
	public final int CMD_REQUEST_REVISION_CONTENTS = 0x06;
	public final int CMD_REQUEST_TAGS = 0x07;
	public final int CMD_SEND_PAGE = 0x08;
	public final int CMD_SET_PAUSED = 0x09;
	
	public final int PEER_TYPE_STATIC = 0; // static fileserver; needs subclass to handle
	public final int PEER_TYPE_BLIND = 1; // has knowledge of seed key, but not archive passphrase; can't decipher data
	public final int PEER_TYPE_FULL = 2; // live peer with knowledge of archive passphrase
	
	/** A message can't be sent to the remote peer because this channel hasn't established that we have full read
	 * access to the archive. */
	public class PeerCapabilityException extends Exception {
		private static final long serialVersionUID = 1L;
	}
	
	/** A message can't be sent to the remote peer because we are in the wrong role (client vs. server) */
	public class PeerRoleException extends Exception {
		private static final long serialVersionUID = 1L;
	}

	protected PeerSocket socket;
	protected int peerType;
	protected byte[] sharedSalt;
	protected HashSet<Long> announcedTags;
	protected boolean remotePaused;
	boolean sentProof;
	
	public PeerConnection(PeerSwarm swarm, String address) throws UnsupportedProtocolException {
		this.socket = PeerSocket.connectToAddress(swarm, address);
	}
	
	public PeerSocket getSocket() {
		return socket;
	}
	
	public void sendAccessProof() throws PeerRoleException {
		assertClientStatus(true);
		ArchiveAccessor accessor = socket.getSwarm().getArchive().getConfig().getAccessor();
		byte[] payload = accessor.temporalProof(0, socket.getSharedSecret());
		send(CMD_ACCESS_PROOF, payload);
	}
	
	public void announceTags(RefTag[] tags) {
		send(CMD_ACCESS_PROOF, serializeRefTags(tags));
	}
	
	public void announceTips() {
		// TODO P2P: define an object for these signed branch tips...
	}
	
	public void requestAll() {
		send(CMD_REQUEST_ALL, new byte[0]);
	}
	
	/** Request all pages pertaining to a given reftag (including merkle tree chunks). */
	public void requestRefTags(RefTag[] refTags) throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REF_TAGS, serializeRefTags(refTags));
	}
	
	public void requestRevisionContents(RefTag[] tips) throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
		send(CMD_REQUEST_REVISION_CONTENTS, serializeRefTags(tips));
	}
	
	public void sendPage(RefTag tag) throws IOException {
		File file = tag.getArchive().getStorage().open(Page.pathForTag(tag.getBytes()), File.O_RDONLY);
		new PeerMessageOutgoing(this, (byte) CMD_SEND_PAGE, tag, file);
	}
	
	public void setPaused(boolean paused) {
		send(CMD_SET_PAUSED, new byte[] { (byte) (paused ? 0x01 : 0x00) });
	}
	
	protected byte[] serializeRefTags(RefTag[] tags) {
		ByteBuffer buf = ByteBuffer.allocate(tags.length * RefTag.REFTAG_SHORT_SIZE);
		for(RefTag tag : tags) buf.put(tag.getBytes());
		return buf.array();
	}
	
	protected void assertPeerCapability(int capability) throws PeerCapabilityException {
		if(peerType < capability) throw new PeerCapabilityException();
	}
	
	protected void assertClientStatus(boolean mustBeClient) throws PeerRoleException {
		if(this.socket.isClient() != mustBeClient) throw new PeerRoleException();
	}
	
	protected void assertState(boolean state) throws ProtocolViolationException {
		if(!state) throw new ProtocolViolationException();
	}
	
	protected byte[] proofResponse(byte[] proof) throws ProtocolViolationException {
		assertState(proof.length == socket.swarm.archive.getCrypto().hashLength());
		// temporalProof returns random garbage if we are not a full peer
		if(Arrays.equals(proof, socket.swarm.archive.getConfig().getAccessor().temporalProof(0, socket.getSharedSecret()))) {
			peerType = PEER_TYPE_FULL;
			return socket.swarm.archive.getConfig().getAccessor().temporalProof(1, socket.getSharedSecret());
		} else {
			// either their proof is garbage (meaning they're blind), or we're a blind seed; send garbage
			peerType = PEER_TYPE_BLIND;
			return socket.swarm.archive.getCrypto().rng(proof.length);
		}
	}

	@Override
	public void handle(PeerMessageIncoming msg) throws ProtocolViolationException, EOFException {
		try {
			switch(msg.cmd) {
			case CMD_ACCESS_PROOF:
				handleAccessProof(msg);
				break;
			case CMD_ANNOUNCE_PEERS:
				handleAnnouncePeers(msg);
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
			case CMD_REQUEST_REF_TAGS:
				handleRequestRefTags(msg);
				break;
			case CMD_REQUEST_REVISION_CONTENTS:
				handleRequestRevisionContents(msg);
				break;
			case CMD_REQUEST_TAGS:
				handleRequestTags(msg);
				break;
			case CMD_SEND_PAGE:
				handleSendPage(msg);
				break;
			case CMD_SET_PAUSED:
				handleSetPaused(msg);
				break;
			default:
				throw new ProtocolViolationException();
			}
		} catch(PeerRoleException | PeerCapabilityException | IOException exc) {
			/* Arguably, blacklisting people because we had a local IOException is unfair. But, there are two real
			 * possibilities here:
			 *   1. They're doing something nasty that's triggering IOExceptions. THey deserve it.
			 *   2. They're not doing anything nasty. In which case, we're getting IOExceptions doing normal stuff, so
			 *      it's not like we can participate as peers right now anyway.
			 * But then... how long is a blacklist entry good for? We might cut ourselves off from a swarm because we
			 * had a mount become temporarily inaccessible or something.
			 */
			throw new ProtocolViolationException();
		}
	}
	
	@Override
	public void establishedSalt(byte[] sharedSalt) {
		this.sharedSalt = sharedSalt;
	}
	
	protected void handleAccessProof(PeerMessageIncoming msg) throws PeerRoleException, ProtocolViolationException, EOFException {
		ArchiveAccessor accessor = this.socket.swarm.archive.getConfig().getAccessor();
		int theirStep = this.socket.isClient() ? 0x01 : 0x00;
		byte[] expected = accessor.temporalProof(theirStep, this.socket.getSharedSecret());
		byte[] received = msg.rxBuf.read(expected.length);
		msg.rxBuf.requireEOF();
		
		peerType = Arrays.equals(expected, received) ? PEER_TYPE_FULL : PEER_TYPE_BLIND;
		if(!socket.isClient()) {
			byte[] response = peerType == PEER_TYPE_FULL 
					? accessor.temporalProof(0x01, socket.getSharedSecret())
					: socket.swarm.archive.getCrypto().prng(this.socket.getSharedSecret()).getBytes(expected.length);
			send(CMD_ACCESS_PROOF, response);
		}
	}
	
	protected void handleAnnouncePeers(PeerMessageIncoming msg) throws EOFException, ProtocolViolationException {
		while(!msg.rxBuf.isEOF()) {
			int urlLen = Util.unsignByte(msg.rxBuf.get());
			assertState(urlLen > 0);
			byte[] urlBytes = msg.rxBuf.read(urlLen);
			socket.swarm.addPeer(new String(urlBytes));
		}
	}
	
	protected void handleAnnounceTags(PeerMessageIncoming msg) throws EOFException {
		assert(RefTag.REFTAG_SHORT_SIZE == 8); // This code depends on tags being sent as 64-bit values.
		while(!msg.rxBuf.isEOF()) {
			Long shortTag = msg.rxBuf.getLong();
			synchronized(this) {
				announcedTags.add(shortTag);
			}
		}
	}
	
	protected void handleAnnounceTips(PeerMessageIncoming msg) {
		// TODO P2P: decide how these are encoded
	}
	
	protected void handleRequestAll(PeerMessageIncoming msg) throws ProtocolViolationException {
		msg.rxBuf.requireEOF();
		// TODO P2P: OK to start sending missing pages
	}
	
	protected void handleRequestRefTags(PeerMessageIncoming msg) throws EOFException, PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
		byte[] shortTag = new byte[RefTag.REFTAG_SHORT_SIZE];
		while(!msg.rxBuf.isEOF()) {
			msg.rxBuf.get(shortTag);
			// TODO P2P: expand the reftag to full length, find all its pages, enqueue to send
		}
	}
	
	protected void handleRequestRevisionContents(PeerMessageIncoming msg) throws EOFException, PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
		byte[] shortTag = new byte[RefTag.REFTAG_SHORT_SIZE];
		while(!msg.rxBuf.isEOF()) {
			msg.rxBuf.get(shortTag);
			// TODO P2P: Broader question: how to stop abusive clients from opening a zillion requests / leaving messages incomplete and abandoning?
			// TODO P2P: expand the reftag to full length, open the ZKFS, enqueue all pages to send
		}
	}
	
	protected void handleRequestTags(PeerMessageIncoming msg) throws EOFException {
		byte[] shortTag = new byte[RefTag.REFTAG_SHORT_SIZE];
		while(!msg.rxBuf.isEOF()) {
			msg.rxBuf.get(shortTag);
			// TODO P2P: expand the reftag to full length, find it on disk, enqueue to send
		}
	}
	
	protected void handleSendPage(PeerMessageIncoming msg) throws IOException, ProtocolViolationException {
		byte[] tagData = msg.rxBuf.read(RefTag.blank(socket.swarm.archive).getBytes().length);
		int expectedChunks = (int) Math.ceil(((double) socket.swarm.archive.getConfig().getPageSize())/PeerMessage.FILE_CHUNK_SIZE);
		ChunkableFileHandle handle = socket.swarm.fileHandleForTag(new RefTag(socket.swarm.archive, tagData));
		
		try {
			while(!msg.rxBuf.isEOF()) {
				long offset = Util.unsignInt(msg.rxBuf.getInt());
				assertState(0 <= offset && offset < expectedChunks && offset <= Integer.MAX_VALUE);
				byte[] chunkData = msg.rxBuf.read(PeerMessage.FILE_CHUNK_SIZE);
				handle.writeChunk((int) offset, chunkData);
				// TODO P2P: how to detect liar peers that send garbage data? we know how to detect a bad page, but how about a bad chunk?
			}
		} catch(EOFException exc) {} // we're allowed to cancel these transfers at any time, causing EOF; just ignore it
	}
	
	protected void handleSetPaused(PeerMessageIncoming msg) throws ProtocolViolationException, EOFException {
		byte pausedByte = msg.rxBuf.get();
		assertState(pausedByte == 0x00 || pausedByte == 0x01);
		msg.rxBuf.requireEOF();
		setRemotePaused(pausedByte == 0x01);
	}
	
	protected void send(int cmd, byte[] payload) {
		assert(cmd > 0 && cmd <= Byte.MAX_VALUE);
		new PeerMessageOutgoing(this, (byte) cmd, payload);
	}
	
	public boolean isPausable(byte cmd) {
		return cmd == CMD_SEND_PAGE;
	}
	
	public boolean wantsFile(RefTag tag) {
		return !announcedTags.contains(tag.getShortHash());
	}
	
	public boolean wantsChunk(RefTag tag, int chunkIndex) {
		if(!wantsFile(tag)) return false;
		return true; // TODO P2P: implement
	}
	
	public synchronized void setRemotePaused(boolean paused) {
		this.remotePaused = paused;
		this.notifyAll();
	}

	public synchronized void waitForUnpause() {
		while(remotePaused) {
			try {
				this.wait();
			} catch(InterruptedException e) {}
		}
	}
}
