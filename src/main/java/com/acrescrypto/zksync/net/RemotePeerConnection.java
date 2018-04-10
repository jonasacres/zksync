package com.acrescrypto.zksync.net;

import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.net.ZKSyncCommand;

public class RemotePeerConnection {
	protected CryptoSupport crypto;
	protected PeerNetSocket socket;
	protected RemotePeerConnectionCallback callback;
	protected byte[] salt_a_precommit;
	protected byte[] salt_a, salt_b;
	protected byte[] salt;
	protected short nextId;
	
	public enum RemotePeerStatus {
		STATUS_SELECTED_ARCHIVE,
		STATUS_ANNOUNCED_TIP_FILE,
		STATUS_ANNOUNCED_TIP,
		
	}
	
	public interface RemotePeerConnectionCallback {
		public void receivedRequest(RemoteRequest req);
		public void receivedResponse(RemoteRequest resp);
	}
	
	public RemotePeerConnection(CryptoSupport crypto, PeerNetSocket socket) {
		this.crypto = crypto;
		this.socket = socket;
		socket.setListener((byte[] message) -> {
			RemoteRequest req = new RemoteRequest(this, message);
			switch(req.cmd) {
			case RemoteRequest.CMD_HANDSHAKE1:
				handleHandshake1(req);
				break;
			case RemoteRequest.CMD_HANDSHAKE2:
				handleHandshake2(req);
				break;
			default:
				callback.receivedRequest(req);
			}
		});
	}
	
	public void violation() {
		// TODO: record a violation for this connection, bail and blacklist
	}
	
	public void disconnect() {
	}
	
	public void requestBranchTips() {
	}
	
	public void requestRefTag(byte[] tag) {
		// TODO: Implement
	}
	
	public void requestRefTagBlocking(byte[] tag) {
	}
	
	public short nextId() {
		return nextId++;
	}
	
	protected void handshake() {
		sendHandshake1().handle((RemoteRequest hs1Req) -> {
			this.salt_b = hs1Req.outgoingStream;
			makeSalt();
			sendHandshake2().handle((RemoteRequest hs2Req) -> {
				// TODO: response contains bloom filter of match IDs
			});
		});
	}
	
	protected RemoteRequest sendHandshake1() {
		this.salt_a  = crypto.rng(32);
		return new RemoteRequest(this, ZKSyncCommand.CMD_HANDSHAKE1, crypto.hash(this.salt_a));
	}
	
	protected RemoteRequest sendHandshake2() {
		return new RemoteRequest(this, ZKSyncCommand.CMD_HANDSHAKE2, this.salt_a);
	}
	
	protected void handleHandshake1(RemoteRequest req) {
		this.salt_a_precommit = req.outgoingStream;
		this.salt_b = crypto.rng(32);
		req.respond(this.salt_b);
	}
	
	protected void handleHandshake2(RemoteRequest req) {
		securityAssertion(Arrays.equals(crypto.hash(req.received), this.salt_a_precommit), "Client salt did not match precommitment");
		this.salt_a = req.received;
		makeSalt();
		req.respond(new byte[0]);
	}
	
	protected void securityAssertion(boolean condition, String message) {
		if(!condition) throw new SecurityException(message);
	}
	
	protected byte[] makeMatchId(ZKArchive archive) {
		return crypto.authenticate(salt, archive.getPubConfig().getArchiveId());
	}
	
	protected void makeSalt() {
		ByteBuffer saltBuf = ByteBuffer.allocate(this.salt_a.length + this.salt_b.length);
		saltBuf.put(this.salt_a);
		saltBuf.put(this.salt_b);
		this.salt_a_precommit = this.salt_b = this.salt_a = null;
		this.salt = crypto.hash(saltBuf.array());
	}
}
