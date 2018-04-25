package com.acrescrypto.zksync.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;

import com.acrescrypto.zksync.LambdaInputStream;
import com.acrescrypto.zksync.MetaInputStream;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKFileCiphertextStream;
import com.acrescrypto.zksync.net.PeerSocket.PeerSocketDelegate;

public class PeerConnection implements PeerSocketDelegate {
	public final byte CMD_ACCESS_PROOF = 0x00;
	public final byte CMD_REQUEST_TIPS = 0x01;
	public final byte CMD_REQUEST_REF_TAG = 0x02;
	public final byte CMD_REQUEST_PAGES = 0x03;
	public final byte CMD_REQUEST_PAGES_IN_RANGE = 0x04;
	public final byte CMD_REQUEST_PEERS = 0x05;
	public final byte CMD_ANNOUNCE_TIP = 0x06;
	
	public final int PEER_TYPE_STATIC = 0; // static fileserver; needs subclass to handle
	public final int PEER_TYPE_BLIND = 1; // has knowledge of seed key, but not archive passphrase; can't decipher data
	public final int PEER_TYPE_FULL = 2; // live peer with knowledge of archive passphrase
	
	public interface PeerConnectionDelegate {
		void discoveredTip(PeerConnection conn, RefTag tag);
		void receivedRefTag(PeerConnection conn, RefTag tag);
		void receivedPage(PeerConnection conn, byte[] tag);
		void receivedTipFile(PeerConnection conn, byte[] contents);
	}
	
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
	protected PeerConnectionDelegate delegate;
	protected byte[] sharedSalt;
	
	public PeerConnection(PeerSwarm swarm, String address) throws UnsupportedProtocolException {
		this.socket = PeerSocket.connectToAddress(swarm, address);
	}
	
	public void sendAccessProof() throws PeerRoleException {
		assertClientStatus(true);
		// if our archive has read access: send proof. else: send rng garbage.
	}
	
	/** Request branch tips file */
	public void requestTips() {
	}
	
	/** Request all pages pertaining to a given reftag (including merkle tree chunks). */
	public void requestRefTags(byte[][] refTags) throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
	}
	
	/** Request pages from the archive. */
	public void requestPages(byte[][] pageTags) {
	}
	
	/** Request pages within indicated range (inclusive) 
	 * @throws PeerCapabilityException */
	public void requestPagesInRange(byte[] lower, byte[] upper) throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_BLIND);
	}
	
	/** Request peer listing 
	 * @throws PeerCapabilityException */
	public void requestPeers() throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_BLIND);
	}
	
	/** Announce the creation/receipt of a new branch tip */
	public void announceTip(byte[] tipTag) throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
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
		if(Arrays.equals(proof, null)) { // TODO: actual archive proof A
			peerType = PEER_TYPE_FULL;
			return null; // TODO: actual archive proof B
		} else {
			// either their proof is garbage (meaning they're blind), or we're a blind seed; send garbage
			peerType = PEER_TYPE_BLIND;
			return socket.swarm.archive.getCrypto().rng(proof.length);
		}
	}

	@Override
	public void receivedMessage(PeerMessage msg) throws ProtocolViolationException {
		try {
			switch(msg.cmd) {
			case CMD_ACCESS_PROOF:
				handleAccessProof(msg);
				break;
			case CMD_REQUEST_TIPS:
				handleRequestTips(msg);
				break;
			case CMD_REQUEST_REF_TAG:
				handleRequestRefTag(msg);
				break;
			case CMD_REQUEST_PAGES:
				handleRequestPages(msg);
				break;
			case CMD_REQUEST_PAGES_IN_RANGE:
				handleRequestPagesInRange(msg);
				break;
			case CMD_REQUEST_PEERS:
				handleRequestPeers(msg);
				break;
			case CMD_ANNOUNCE_TIP:
				handleAnnounceTip(msg);
				break;
			default:
				throw new ProtocolViolationException();
			}
		} catch(PeerRoleException | PeerCapabilityException exc) {
			throw new ProtocolViolationException();
		}
	}
	
	@Override
	public void establishedSalt(byte[] sharedSalt) {
		this.sharedSalt = sharedSalt;
	}
	
	public void handleAccessProof(PeerMessage msg) throws PeerRoleException, ProtocolViolationException {
		assertClientStatus(false);
		msg.await((ByteBuffer data) -> {
			assertState(data.remaining() == socket.swarm.archive.getCrypto().hashLength());
			byte[] proof = new byte[data.remaining()];
			data.get(proof);
			msg.respond(new ByteArrayInputStream(proofResponse(proof)));
		});
	}
	
	public void handleRequestTips(PeerMessage msg) {
		try {
			String path = socket.swarm.archive.getRevisionTree().getPath();
			msg.respond(socket.swarm.archive.getStorage().open(path, File.O_RDONLY).getInputStream());
		} catch(IOException exc) {
			msg.respond(new ByteArrayInputStream(new byte[0]));
		}
	}
	
	/** TODO: this means we get just one stream from a single (possibly slow) peer... what if we let clients break it up?
	 * RefTag gives length, so request bitmask showing the pages we want
	 * respond with bitmask of chunks we offer, followed by ordered stream of pages
	 * controller ensures each peer has a few pages at a time
	 * 
	 * 64-bit starting offset
	 * Bitmask
	 * @param msg
	 * @throws PeerCapabilityException
	 */
	public void handleRequestRefTag(PeerMessage msg) throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
		msg.await((ByteBuffer data) -> {
			assertState(data.remaining() == socket.swarm.archive.refTagSize());
			RefTag tag = new RefTag(socket.swarm.archive, data.array());
			assertState(tag.getRefType() != RefTag.REF_TYPE_IMMEDIATE);
			
			try {
				ByteBuffer pageCount = ByteBuffer.allocate(8);
				pageCount.putLong(tag.getNumPages());
				MetaInputStream contents = new MetaInputStream(new InputStream[] {
						new ByteArrayInputStream(pageCount.array()),
						new ZKFileCiphertextStream(tag)
				});
				msg.respond(contents);
			} catch (IOException e) {
				msg.respond(new ByteArrayInputStream(new byte[0])); // send empty response for reftags we don't have
			}
		});
	}
	
	// TODO: would we be better off replacing requestPages and requestPagesInRange with requestPagesWithPrefix?
	public void handleRequestPages(PeerMessage msg) {
		msg.await((ByteBuffer data) -> {
			MetaInputStream pages = new MetaInputStream();
			byte[] pageTag = new byte[socket.swarm.archive.getCrypto().hashLength()];
			assertState(data.remaining() % pageTag.length == 0);
			while(data.remaining() > 0) {
				data.get(pageTag);
				try {
					pages.add(pageTag);
					pages.add(socket.swarm.archive.getStorage().open(Page.pathForTag(pageTag), File.O_RDONLY).getInputStream());
				} catch (IOException e) {
					// just skip pages we don't have
				}
			}
			msg.respond(pages);
		});
	}
	
	public void handleRequestPagesInRange(PeerMessage msg) {
		msg.await((ByteBuffer data) -> {
			byte[] startTag = new byte[socket.swarm.archive.getCrypto().hashLength()];
			byte[] endTag = new byte[startTag.length];
			assertState(data.remaining() == 2*startTag.length);
			data.get(startTag);
			data.get(endTag);
			try {
				Iterator<String> pathIterator = socket.swarm.archive.getStorage().opendir("/").listRecursiveIterator(Directory.LIST_OPT_OMIT_DIRECTORIES);
				LambdaInputStream response = new LambdaInputStream(() -> {
					if(!pathIterator.hasNext()) return null;
					try {
						return socket.swarm.archive.getStorage().open(pathIterator.next(), File.O_RDONLY).getInputStream();
					} catch (IOException e) {
						return null;
					}
				});
				
				msg.respond(response);
			} catch (IOException e) {
				msg.respond(new ByteArrayInputStream(new byte[0]));
			}
		});
	}
	
	public void handleRequestPeers(PeerMessage msg) {
		int length = 0;
		for(String peer : socket.swarm.knownPeers) length += peer.length()+2;
		ByteBuffer buf = ByteBuffer.allocate(length);
		for(String peer : socket.swarm.knownPeers) {
			buf.putShort((short) peer.getBytes().length);
			buf.put(peer.getBytes());
		}
		msg.respond(new ByteArrayInputStream(buf.array()));
	}
	
	public void handleAnnounceTip(PeerMessage msg) throws PeerCapabilityException {
		assertPeerCapability(PEER_TYPE_FULL);
		msg.await((ByteBuffer data) -> {
			assertState(data.remaining() == socket.swarm.archive.refTagSize());
			// TODO: do we really want to automatically download every new tip? either way... what do we do next?
		});
	}
}
