package com.acrescrypto.zksync.net;

public abstract class PeerMessage {
	public final static int HEADER_LENGTH = 4 + 4 + 1 + 1 + 2; // msgId[4] + payloadLen[4] + cmd[1] + flags[1] + reserved[2]
	public final static int MESSAGE_SIZE = 8192 + HEADER_LENGTH;
	public final static int FILE_CHUNK_SIZE = MESSAGE_SIZE - HEADER_LENGTH - 4; // leave 4 bytes for a chunk index
	public final static byte FLAG_FINAL = 0x01; // no further messages should be sent/expected in this message ID

	protected int msgId;
	protected byte cmd;
	protected byte flags;

	protected PeerConnection connection;
}
