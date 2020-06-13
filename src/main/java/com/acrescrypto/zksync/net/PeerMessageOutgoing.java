package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PeerMessageOutgoing extends PeerMessage {
	public interface PeerMessageOutgoingDataProvider {
		public ByteBuffer nextBytes(MutableBoolean expectMore) throws IOException;
	}
	
	protected PeerMessageOutgoingDataProvider dataProvider,
	                                          followupDataProvider;
	protected boolean                         aborted,
	                                          completed,
	                                          managedPayloadsPending;
	protected ByteBuffer                      currentPayload;
	protected Queue<ByteBuffer>               managedPayloads;
	private   Logger                          logger           = LoggerFactory.getLogger(PeerMessageOutgoing.class);
	
	public PeerMessageOutgoing(
			PeerConnection                  connection,
			int                             msgId,
			byte                            cmd,
			byte                            flags,
			PeerMessageOutgoingDataProvider dataProvider
	) {
		this.connection      = connection;
		this.cmd             = cmd;
		this.flags           = flags;
		this.dataProvider    = dataProvider;
		this.msgId           = msgId;
	}
	
	public PeerMessageOutgoing(
			PeerConnection                  connection,
			byte                            cmd
	) {
		this.connection      = connection;
		this.cmd             = cmd;
		this.flags           = (byte) 0;
		this.msgId           = Integer.MIN_VALUE;
		this.managedPayloads = new LinkedList<>();
		setManagedDataProvider();
	}
	
	public PeerMessageOutgoing(
			PeerConnection                  connection,
			byte                            cmd,
			PeerMessageOutgoingDataProvider dataProvider
	) {
		this(connection, Integer.MIN_VALUE, cmd, (byte) 0, dataProvider);
	}
	
	public void abort() {
		aborted = true;
	}
	
	public int maxPayloadLength() {
		return PeerMessage.MESSAGE_SIZE;
	}
	
	public void send(ByteBuffer data, PeerMessageOutgoingDataProvider provider) throws IOException {
		followupDataProvider = provider;
		send(data, true);
	}
	
	public void send(ByteBuffer data, boolean hasMore) throws IOException {
		setManagedDataProvider();
		managedPayloads.add(data);
		managedPayloadsPending = hasMore;
		
		if(managedPayloads.size() == 1) {
			transmit();
		}
	}
	
	protected void setManagedDataProvider() {
		this.followupDataProvider = null;
		this.dataProvider         = (more) -> {
			more.setValue(managedPayloadsPending);
			ByteBuffer payload = managedPayloads.poll();
			if(managedPayloads.isEmpty() && followupDataProvider != null) {
				this.dataProvider = followupDataProvider;
			}
			
			return payload;
		};
	}
	
	public void finish() throws IOException {
		if(completed) return;
		
		managedPayloadsPending = false;
		if(managedPayloads.isEmpty()) {
			transmit();
		}
	}
	
	public boolean isFinished() {
		return completed;
	}
	
	public void transmit() throws IOException {
		/* TODO: abort makes a hard stop in transmitting a message without sending a
		 * FLAG_FINAL. Review if this is appropriate.
		 */
		if(aborted  )  return;
		if(completed)  return;
		if(connection.isPausable(cmd) && connection.isPaused()) {
			connection.onUnpause( () -> transmit() );
			return;
		}
		
		MutableBoolean expectMore   = new MutableBoolean(false);
		ByteBuffer     payload      = currentPayload.hasRemaining()
		                              ? currentPayload
		                              : dataProvider != null
				                        ? dataProvider.nextBytes(expectMore)
				                        : null;
		ByteBuffer     header       = ByteBuffer.allocate(HEADER_LENGTH);
		boolean        hasMoreBytes = expectMore.isTrue();
		boolean        hasPayload   = payload != null
				                   && payload.hasRemaining();
		int            length       = hasPayload
				                      ? 0
				                      : Math.min(
				                    		  maxPayloadLength(),
				                    		  payload.remaining()
				                    	);
		byte           msgFlags     = flags;
		boolean        pauseForNow  = hasMoreBytes && !hasPayload;
		
		if( pauseForNow) return; // still waiting on next bytes; someone will call transmit() later to continue/finish
		if( completed  ) return; // in case dataProvider caused us to send something else

		if(!hasMoreBytes) {
			msgFlags |= FLAG_FINAL;
			completed = true;
		}
		
		header.putInt  (msgId);
		header.putInt  (length);
		header.put     (cmd);
		header.put     (msgFlags);
		header.putShort((short) 0);
		
		// last chance to bail if someone aborted this message
		if( aborted    )      return;
		
		connection.getSocket().write(header);
		if(!hasPayload)       return;
		
		// send only the next `length` bytes
		ByteBuffer effectivePayload = ByteBuffer.wrap(
				payload.array   (),
				payload.position(),
				payload.position() + length);
		payload.position(payload.position() + length);
		
		connection.getSocket().write(effectivePayload, ()->{
			if(!hasMoreBytes) return;
			transmit();
		});
	}
	
	public boolean equals(Object other) {
		// TODO API: (coverage) method / consider if necessary
		if(other instanceof Integer) {
			return other.equals(msgId);
		}
		
		if(other instanceof PeerMessageOutgoing) {
			return ((PeerMessageOutgoing) other).msgId == msgId;
		}
		
		return false;
	}
}
