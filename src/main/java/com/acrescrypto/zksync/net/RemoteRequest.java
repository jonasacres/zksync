package com.acrescrypto.zksync.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class RemoteRequest {
	protected interface RemoteRequestResponseHandler {
		public void handle(RemoteRequest req);
	}
	
	public final static byte CMD_HANDSHAKE1 = 0x00;        // handshake1(byte[] salt_a_precommit) -> salt_b
	public final static byte CMD_HANDSHAKE2 = 0x01;        // handshake2(byte[] salt_a) -> archive match_id bloom (match_id = H(salt, seed_id))

	/* revisit this. both peers should PROVE full peer status if applicable at this stage. */
	public final static byte CMD_SELECT_ARCHIVE = 0x02;    // select-archive(byte[] match_id) -> [result, confirm_id] (0 = no archive, 1 = full peer, 2 = blind seed), confirm_id = H(seed_id, salt)

	public final static byte CMD_AUTH_PASSPHRASE = 0x03;   // passphrase-auth(char[] username, byte[] passhash) -> [result, permissions], passhash = H(salt, argon2(passphrase))
	public final static byte CMD_AUTH_TOKEN = 0x04;        // cookie-auth(byte[] cookie) -> [result, permissions]
	public final static byte CMD_OPEN_SIDECHANNEL = 0x05;  // open-sidechannel(int8 numChannels) -> request permission to create side channel, return cookie and max number of channels

	/* any peer, no authentication needed */
	public final static byte CMD_GET_PAGE = 0x06;          // get-page(byte[] tag) -> return 1 or more specific pages. needed for random access of file.
	public final static byte CMD_GET_TIPS = 0x07;          // get-tips() -> return revisiontree file
	public final static byte CMD_GET_RANGE = 0x08;         // get-range(byte[] min, byte[] max) -> stream all pages in [min, max). needed for initial mass sync of archive
	public final static byte CMD_ANNOUNCE_TIPS = 0x09;     // announce-tips(byte[] hash) -> no response, notify peer that we have new tips that can be requested, include simple hash of file ciphertext.
	public final static byte CMD_ANNOUNCE_TIP = 0x0a;      // announce-tip(byte[] reftag) -> no response, notify peer that we have new tip. send to full peers only instead of announce-tips.

	/* maybe i want these, maybe i don't... needs more math/simulation. needed for rapid sync of blind seeds. would be nice to find something better... */
	public final static byte CMD_BLOOM_SYNC = 0x0b;        // sync using iterative bloom method. require auth due to computational cost
	public final static byte CMD_PUSH_BLOOM_SYNC = 0x0c;   // cause receiver to request a bloom sync

	/* full peers only, no authentication needed */
	public final static byte CMD_GET_TAG = 0x0d;           // get-tag(byte[] tag) -> return all pages of file with reftag. needed for random access of file.

	/* authenticated write peers only */
	public final static byte CMD_PUSH_PAGE = 0x0e;         // push-page(byte[] tag, byte[] contents) -> void. Store a page.
	public final static byte CMD_PUSH_TIPS = 0x0f;         // push-tips(byte[] contents) -> void. Store branch tips file.
	
	// CMD_CANCEL to abort transmission of a given id?

	public final static byte FLAG_RESPONSE = 0x01;         // message is a response to an earlier request
	public final static byte FLAG_FINAL = 0x02;            // this chunk will be the last sent by this peer for this id
	public final static byte FLAG_CANCEL = 0x04;           // no further chunks should be sent by remote peer for this id
	
	protected byte cmd;
	protected byte flags;
	protected byte[] received;
	protected InputStream outgoingStream;
	protected OutputStream readStream;
	protected short id;
	protected RemotePeerConnection conn;
	protected RemoteRequestResponseHandler handler;
	
	/**
	 * 
	 */
	
	// TODO: how to sync branch tips through blind seed?
	
	/** make a new incoming request or response whose header (and optional payload) are in the supplied byte array */
	public RemoteRequest(RemotePeerConnection conn, byte[] message) {
		this.conn = conn;
		ByteBuffer buf = ByteBuffer.wrap(message);
		this.cmd = buf.get();
		this.flags = buf.get();
		this.id = buf.getShort();
		
		this.received = new byte[buf.remaining()];
		buf.get(this.received, 0, buf.remaining());
	}
	
	/** make a new outgoing request whose payload is in the supplied byte array */
	public RemoteRequest(RemotePeerConnection conn, byte cmd, byte[] payload) {
		this(conn, cmd, new ByteArrayInputStream(payload));
	}
	
	/** make a new outgoing request whose payload is in the supplied byte array */
	public RemoteRequest(RemotePeerConnection conn, byte cmd, InputStream payloadStream) {
		this.conn = conn;
		this.cmd = cmd;
		this.id = conn.nextId();
		this.outgoingStream = payloadStream;
	}
	
	public RemoteRequest send() {
		// TODO: send the damn thing
		return this;
	}
	
	public RemoteRequest waitForResponse() {
		// TODO: wait for the response
		return this;
	}
	
	public byte[] getRemoteData() {
		return received;
	}
	
	/** true if the message we received is a request (FLAG_RESPONSE not set) */
	public boolean isRequest() {
		return (flags & FLAG_RESPONSE) == 0;
	}
	
	public void respond(byte[] response) {
		this.outgoingStream = new ByteArrayInputStream(response);
		// TODO: send over network
	}
	
	public RemoteRequest handle(RemoteRequestResponseHandler handler) {
		this.handler = handler;
		return this;
	}
	
	public int getCmd() {
		return cmd;
	}
	
	protected void receivedBytes(byte[] incoming) {
		try {
			readStream.write(incoming);
		} catch (IOException e) {
			// TODO: cancel request
			e.printStackTrace();
		}
	}
}
