package com.acrescrypto.zksync.net.noise;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;

public class VariableLengthHandshakeState extends HandshakeState {
	protected SimplePayloadReader simplePayloadReader;
	protected byte[] prehash;
	
	public interface SimplePayloadReader {
		void readPayload(int round, byte[] payload);
	}

	public VariableLengthHandshakeState(CryptoSupport crypto, String protocolName, String handshakePattern,
			boolean isInitiator, byte[] prologue, PrivateDHKey localStaticKey, PrivateDHKey localEphemeralKey,
			PublicDHKey remoteStaticKey, PublicDHKey remoteEphemeralKey, byte[] psk) {
		super(crypto, protocolName, handshakePattern, isInitiator, prologue, localStaticKey, localEphemeralKey, remoteStaticKey,
				remoteEphemeralKey, psk);
	}
	
	@Deprecated
	public void setPayload(PayloadWriter writer, PayloadReader reader) {
		throw new RuntimeException("Cannot use ordinary PayloadReader on VariableLengthHandshakeState");
	}
	
	public void setSimplePayload(PayloadWriter writer, SimplePayloadReader reader) {
		this.simplePayloadReader = reader;
		this.payloadWriter = writer;
	}
	
	protected void handleWritePayload(OutputStream out) throws IOException {
		byte[] payload = null;
		
		if(payloadWriter != null) {
			payload = payloadWriter.writePayload(round);
		}

		byte[] terminator = terminator();
		
		if(payload != null) {
			out.write(symmetricState.encryptAndHash(payload));
		}
		
		symmetricState.mixHash(terminator);
		
		out.write(terminator);
	}
	
	protected void handleReadPayload(InputStream in) throws IOException {
		ByteBuffer buf = ByteBuffer.allocate(65536);
		byte[] terminator = terminator();
		prehash = getHash().clone();
		
		do {
			int numRead = in.read(buf.array(), buf.position(), 1);
			if(numRead < 0) throw new EOFException();
			buf.position(buf.position() + numRead);
		} while(!complete(buf, terminator));
		
		byte[] payload = null;
		int ciphertextLen = buf.position() - terminator.length;
		if(ciphertextLen > 0) {
			byte[] trimmed = new byte[ciphertextLen];
			System.arraycopy(buf.array(), 0, trimmed, 0, trimmed.length);
			payload = symmetricState.decryptAndHash(trimmed);
		}
		
		symmetricState.mixHash(terminator);
		
		if(simplePayloadReader != null) {
			simplePayloadReader.readPayload(round, payload);
		}
	}
	
	protected boolean complete(ByteBuffer buf, byte[] terminator) {
		if(buf.position() < terminator.length) return false;
		
		byte[] array = buf.array();
		int start = buf.position() - terminator.length;
		
		for(int i = 0; i < terminator.length; i++) {
			if(array[start + i] != terminator[i]) return false;
		}
		
		return true;
	}
	
	protected byte[] terminator() {
		return symmetricState.hkdf(("terminator " + round).getBytes(), 1)[0];
	}
	
	public byte[] getPreHash() {
		return prehash;
	}
}
