package com.acrescrypto.zksync.net.dht;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.ProtocolViolationException;
import com.acrescrypto.zksync.exceptions.UnsupportedProtocolException;
import com.acrescrypto.zksync.net.dht.DHTMessage.DHTMessageCallback;
import com.acrescrypto.zksync.utility.Util;

public class DHTPeer implements Sendable {
	public final static byte FLAG_PINNED = 0x01;
	
	interface DHTFindNodePeerCallback {
		void response(Collection<DHTPeer> references, boolean isFinal);
	}
	
	interface DHTFindNodeRecordCallback {
		void receivedRecord(DHTRecord record);
	}

	protected DHTClient   client;
	protected DHTID       id;
	protected String      address;
	protected long        lastSeen;
	protected int         port;
	protected int         missedMessages;
	protected PublicDHKey key;
	protected byte[]      remoteAuthTag = new byte[DHTClient.AUTH_TAG_SIZE];
	protected boolean     pinned; // do not prune this peer
	protected boolean     verified;
	
	private Logger logger = LoggerFactory.getLogger(DHTPeer.class);
	
	public DHTPeer(DHTClient client, String address, int port, byte[] key) throws UnknownHostException {
		this(
				client,
				address,
				port,
				client.crypto.makePublicDHKey(key)
			);
	}
	
	public DHTPeer(DHTPeer peer) throws UnknownHostException {
		this(peer.client, peer);
	}
	
	public DHTPeer(DHTClient client, DHTPeer peer) throws UnknownHostException {
		this(client, peer.address, peer.port, peer.key);
		this.id              = DHTID.withBytes(peer.id.serialize()); // some tests hijack ID field, so be sure to capture original ID and don't trust key-derived value
		this.lastSeen        = peer.lastSeen;
		this.missedMessages  = peer.missedMessages;
		this.remoteAuthTag   = peer.remoteAuthTag.clone();
		this.pinned          = peer.pinned;
		this.verified        = peer.verified;
	}
	
	public DHTPeer(DHTClient client, String address, int port, PublicDHKey key) throws UnknownHostException {
		this.client  = client;
		this.address = InetAddress.getByName(address).getHostAddress();
		this.port    = port;

		this.key     = key;
		this.id      = DHTID.withKey(key);
	}
	
	public DHTPeer(DHTClient client, ByteBuffer serialized) throws EINVALException {
		this.client = client;
		deserialize(serialized);
	}
	
	public boolean isBad() {
		return missedMessages > 1;
	}
	
	public boolean isQuestionable() {
		long pollIntervalMs = client.getMaster().getGlobalConfig().getLong("net.dht.pollIntervalMs");
		return Util.currentTimeMillis() - lastSeen > pollIntervalMs;
	}
	
	public synchronized void missedMessage() {
		missedMessages++;
	}
	
	public synchronized void acknowledgedMessage() {
		missedMessages = 0;
		lastSeen       = Util.currentTimeMillis();
	}
	
	public void ping() {
		ping(null);
	}
	
	public void ping(DHTMessageCallback callback) {
		logger.debug("DHT {}:{}: send ping",
				address,
				port);
		client.getProtocolManager().pingMessage(this, callback).send();
	}
	
	public void findNode(DHTID nodeId, Key lookupKey, DHTFindNodePeerCallback peerCallback, DHTFindNodeRecordCallback recordCallback) {
		client.getProtocolManager().findNodeMessage(this, nodeId, lookupKey, (resp)->{
			ArrayList<DHTPeer> receivedPeers = new ArrayList<>();
			ByteBuffer buf = ByteBuffer.wrap(resp.payload);
			while(buf.hasRemaining()) {
				if(buf.remaining() < 1+2) throw new ProtocolViolationException();
				
				int listIndex = buf.get();
				int nextLen = Util.unsignShort(buf.getShort());
				if(nextLen <= 0 || nextLen > buf.remaining()) throw new ProtocolViolationException();
				int expectedPos = buf.position() + nextLen;
				
				switch(listIndex) {
				case 0: // peer list item
					DHTPeer peer;
					
					try {
						peer = client.routingTable.canonicalPeer(new DHTPeer(client, buf));
					} catch(EINVALException exc) {
						throw new ProtocolViolationException();
					}
					
					if(expectedPos != buf.position()) throw new ProtocolViolationException();
					
					receivedPeers.add(peer);
					client.routingTable.suggestPeer(peer);
					break;
				case 1: // record list item
					try {
						DHTRecord record = client.getProtocolManager().deserializeRecord(null, buf);
						record.setSender(resp.peer);
						
						recordCallback.receivedRecord(record);
						if(buf.position() != expectedPos) throw new UnsupportedProtocolException();
					} catch (UnsupportedProtocolException e) {
						buf.position(expectedPos);
					}
					break;
				default:
					// TODO API: (coverage) conditional
					buf.position(expectedPos);
				}				
			}
			
			peerCallback.response(receivedPeers, resp.isFinal);
		}).send();
	}
	
	public void addRecord(DHTID recordId, Key lookupKey, DHTRecord record) {
		if(!canVerifyToPeer()) {
			logger.info("DHT {}:{}: Can't addRecord to {} without remote auth tag; pinging",
					address,
					port,
					recordId.toShortString());
			this.ping((msg)->addRecord(recordId, lookupKey, record));
			return;
		}
		
		logger.info("DHT {}:{}: send addRecord {}",
				address,
				port,
				recordId.toShortString());
		client.getProtocolManager().addRecordMessage(
				this,
				recordId,
				lookupKey,
				record,
				null
			 ).send();
	}
	
	public void markVerified() {
		if(verified) return;
		verified = true;
		client.getRoutingTable().verifiedPeer(this);
	}
	
	public boolean isVerified() {
		return verified;
	}
	
	public boolean canVerifyToPeer() {
		if(remoteAuthTag == null) return false;
		
		for(byte b : this.remoteAuthTag) {
			if(b != 0) return true;
		}
		
		return false;
	}
	
	public boolean equals(Object o) {
		if(!(o instanceof DHTPeer)) {
			return id.equals(o);
		}
		
		DHTPeer other = (DHTPeer) o;
		if(!id     .equals(other.id))      return false;
		if(!address.equals(other.address)) return false;
		if( port    !=     other.port)     return false;
		
		return true;
	}
	
	public byte[] localAuthTag() {
		byte[] tag = new byte[DHTClient.AUTH_TAG_SIZE];
		
		byte[] token = client.tagKey.authenticate(Util.concat(
					key.getBytes(),
					Util.serializeInt(port),
					address.getBytes()
				));
		System.arraycopy(
				token,
				0,
				tag,
				0,
				tag.length
			);
		return tag;
	}

	@Override
	public int hashCode() {
		return id.hashCode();
	}

	@Override
	public byte[] serialize() {
		byte flags = 0;
		if(pinned) {
			flags |= FLAG_PINNED;
		}
		
		ByteBuffer buf = ByteBuffer.allocate(
				   2                             // address length
				 + address.getBytes().length     // address
				 + 2                             // port
				 + 2                             // key length
				 + key.getBytes().length         // key
				 + 1                             // flags
			);
		buf.putShort((short) address.getBytes().length);
		buf.put     (        address.getBytes()       );
		buf.putShort((short) port                     );
		buf.putShort((short) key.getBytes().length    );
		buf.put     (        key.getBytes()           );
		buf.put     (        flags                    );
		
		return buf.array();
	}
	
	protected void deserialize(ByteBuffer serialized) throws EINVALException {
		try {
			int addrLen      = Util.unsignShort(serialized.getShort());
			this.address     = new String(serialized.array(),
					                      serialized.position(),
					                      addrLen);
			
			serialized.position(serialized.position() + addrLen);
			this.port        = Util.unsignShort(serialized.getShort());
			
			int keyLen       = Util.unsignShort(serialized.getShort());
			byte[] keyBytes  = new byte[keyLen];
			serialized.get(keyBytes);
			
			this.key         = client.crypto.makePublicDHKey(keyBytes);
			this.id          = DHTID.withKey(this.key);
			
			byte flags       = serialized.get();
			this.pinned      = (flags & FLAG_PINNED) != 0;
		} catch(BufferUnderflowException exc) {
			throw new EINVALException("(dht peer)");
		}
	}
	
	public boolean matches(String address, int port, PublicDHKey key) {
		return this.port == port && this.address.equals(address) && this.key.equals(key);
	}
	
	public DHTID getId() {
		return id;
	}
	
	public PublicDHKey getKey() {
		return key;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getAddress() {
		return address;
	}
	
	public int getMissedMessages() {
		return missedMessages;
	}
	
	public long getLastSeen() {
		return lastSeen;
	}
	
	public boolean isPinned() {
		return pinned;
	}
	
	public void setPinned(boolean pinned) {
		this.pinned = pinned;
	}
	
	public String toString() {
		return Util.bytesToHex(key.getBytes(), 4) + " " + address + ":" + port;
	}
}
