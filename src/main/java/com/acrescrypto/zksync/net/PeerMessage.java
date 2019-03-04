package com.acrescrypto.zksync.net;

public abstract class PeerMessage {
	public final static int HEADER_LENGTH = 4 + 4 + 1 + 1 + 2; // msgId[4] + payloadLen[4] + cmd[1] + flags[1] + reserved[2]
	public final static int MESSAGE_SIZE = 8192 + HEADER_LENGTH;
	public final static int DEFAULT_MAX_OPEN_MESSAGES = 16;
	
	/** I'm not thrilled about MESSAGE_SIZE and FILE_CHUNK_SIZE being kept constant.
	 * There's no "right" value for these, and it's based on how much RAM/bandwidth is available to the
	 * most resource-challenged peer we're willing to support. But how to vary them? MESSAGE_SIZE could
	 * be agreed upon between peers at handshake, but FILE_CHUNK_SIZE needs to be consistent for the
	 * chunk accumulation strategy to work. Or at least, everyone needs to send in multiples of some
	 * minimum chunk size.
	 * 
	 * This can all happen in a later protocol version.
	 */
	public final static int FILE_CHUNK_SIZE = MESSAGE_SIZE - HEADER_LENGTH - 4; // leave 4 bytes for a chunk index
	public final static byte FLAG_FINAL = 0x01; // no further messages should be sent/expected in this message ID
	public final static byte FLAG_CANCEL = 0x02; // please do not send us any more data for message with indicated ID

	protected int msgId;
	protected byte cmd;
	protected byte flags;

	protected PeerConnection connection;
}
