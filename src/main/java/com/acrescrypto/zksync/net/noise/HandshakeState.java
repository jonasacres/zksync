package com.acrescrypto.zksync.net.noise;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.io.IOUtils;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.utility.Util;

// per section 5.3, Noise specification, http://noiseprotocol.org/noise.html, rev 34, 2018-07-11
public class HandshakeState {
	protected CryptoSupport crypto;
	
	protected SymmetricState symmetricState;
	
	protected PrivateDHKey localStaticKey;          // 's' in noise spec
	protected PrivateDHKey localEphemeralKey;       // 'e' in noise spec
	protected PublicDHKey remoteStaticKey;          // 'rs' in noise spec
	protected PublicDHKey remoteEphemeralKey;       // 're' in noise spec
	
	protected boolean isInitiator;                  // 'initiator' in noise spec
	protected Queue<String[]> messagePatterns;      // 'message_patterns' in noise spec
	
	protected HashMap<String,TokenWriteHandler> writeHandlers = new HashMap<>();
	protected HashMap<String,TokenReadHandler> readHandlers = new HashMap<>();
	
	protected byte[] psk;
	
	protected String protocolName;
	
	protected KeyDeobfuscator deobfuscator;
	protected KeyObfuscator obfuscator;
	protected PayloadReader payloadReader;
	protected PayloadWriter payloadWriter;
	
	protected int round;
	
	private interface TokenReadHandler {
		public void handle(InputStream in) throws IOException;
	}
	
	private interface TokenWriteHandler {
		public void handle(OutputStream out) throws IOException;
	}
	
	public interface DecryptHandler {
		public byte[] decrypt(byte[] ciphertext);
	}
	
	public interface PayloadReader {
		/** Callback for handling additional payload in handshaking.
		 * 
		 * @param round Message round. The first message from the initiator is round 1, the response is round 2, the next initiator message is round 3, and so on.
		 * @param in InputStream bearing ciphertext
		 * @param decrypter DecryptHandler for decrypting ciphertext. Returns plaintext. Must be called exactly once on a byte array containing entire ciphertext read from InputStream, or handshaking will fail.
		 * @return Ciphertext bytes read from stream.
		 * @throws IOException
		 */
		public void readPayload(int round, InputStream in, DecryptHandler decrypter) throws IOException;
	}
	
	public interface PayloadWriter {
		/** Callback for providing additional payload in handshaking.
		 * 
		 * @param round Message round. The first message from the initiator is round 1, the response is round 2, the next initiator message is round 3, and so on.
		 * @return Plaintext bytes to be encrypted and sent.
		 */
		public byte[] writePayload(int round);
	}
	
	public interface KeyObfuscator {
		public byte[] obfuscate(PublicDHKey key);
	}

	public interface KeyDeobfuscator {
		public byte[][] deobfuscate(InputStream in) throws IOException;
	}
	
	public HandshakeState(CryptoSupport crypto,
			String protocolName,
			String handshakePattern,
			boolean isInitiator,
			byte[] prologue,
			PrivateDHKey localStaticKey,
			PrivateDHKey localEphemeralKey,
			PublicDHKey remoteStaticKey,
			PublicDHKey remoteEphemeralKey,
			byte[] psk) {
		initTokenMappers();
		this.crypto = crypto;
		
		this.protocolName = protocolName;
		this.symmetricState = new SymmetricState(crypto, protocolName);
		
		this.deobfuscator = (in) -> {
			byte[] key = IOUtils.readFully(in, crypto.asymPublicDHKeySize());
			return new byte[][] { key, key };
		};
		
		this.obfuscator = (key) -> key.getBytes();
		
		this.payloadReader = (round, in, decrypter) -> decrypter.decrypt(new byte[0]);
		this.payloadWriter = (round) -> new byte[0];
		
		if(prologue == null) prologue = new byte[0];
		this.symmetricState.mixHash(prologue);

		this.isInitiator = isInitiator;
		this.localStaticKey = localStaticKey;
		this.localEphemeralKey = localEphemeralKey;
		this.remoteStaticKey = remoteStaticKey;
		this.remoteEphemeralKey = remoteEphemeralKey;
		this.psk = psk;
		
		decodeMessagePattern(handshakePattern);
	}
	
	public void setObfuscation(KeyObfuscator obfuscator, KeyDeobfuscator deobfuscator) {
		this.obfuscator = obfuscator;
		this.deobfuscator = deobfuscator;
	}
	
	public void setPayload(PayloadWriter payloadWriter, PayloadReader payloadReader) {
		this.payloadWriter = payloadWriter;
		this.payloadReader = payloadReader;
	}
	
	public void setPayloadWriter(PayloadWriter payloadWriter) {
		this.payloadWriter = payloadWriter;
	}

	public void setPayloadReader(PayloadReader payloadReader) {
		this.payloadReader = payloadReader;
	}

	protected void decodeMessagePattern(String bigPattern) {
		this.messagePatterns = new LinkedList<>();
		
		String[] lines = bigPattern.split("\n");
		boolean hasPremessage = false;
		for(String line : lines) {
			if(line.trim().equals("...")) {
				hasPremessage = true;
				break;
			}
		}
		
		boolean inPremessage = hasPremessage;
		
		for(String line : lines) {
			line = line.trim();
			if(!line.startsWith("-> ") && !line.startsWith("<- ") && !line.equals("...")) continue;
			
			if(line.equals("...")) {
				inPremessage = false;
				continue;
			}
			
			boolean lineForInitiator = line.startsWith("-> ");
			if(inPremessage) {
				for(String token : line.split(",? ")) {
					if(token.equals("s")) {
						if(lineForInitiator == isInitiator) {
							this.symmetricState.mixHash(localStaticKey.publicKey().getBytes());
						} else {
							this.symmetricState.mixHash(remoteStaticKey.getBytes());
						}
					} else if(token.equals("e")) {
						if(lineForInitiator == isInitiator) {
							this.symmetricState.mixHash(localEphemeralKey.publicKey().getBytes());
						} else {
							this.symmetricState.mixHash(remoteEphemeralKey.getBytes());
						}
					}
				}
			} else {
				messagePatterns.add(line.split(",? "));
			}
		}
	}
	
	public CipherState[] handshake(InputStream in, OutputStream out) throws IOException {
		CipherState[] result = null;
		boolean skip = !isInitiator;
		
		while(result == null) {
			if(!skip) {
				result = writeMessage(out);
			}
			
			if(result == null) {
				result = readMessage(in);
			}
			
			skip = false;
		}
		
		return result;
	}
	
	protected CipherState[] writeMessage(OutputStream out) throws IOException {
		round++;
		String[] tokens = messagePatterns.poll();
		for(String token : tokens) {
			TokenWriteHandler handler = writeHandlers.get(token);
			if(handler != null) {
				handler.handle(out);
			}
		}
		
		byte[] payload = payloadWriter.writePayload(round);
		if(payload != null && payload.length > 0) {
			byte[] ciphertext = symmetricState.encryptAndHash(payload);
			out.write(ciphertext);
		}
		
		if(messagePatterns.isEmpty()) return readyStates();
		return null;
	}
	
	protected CipherState[] readMessage(InputStream in) throws IOException {
		round++;
		
		String[] tokens = messagePatterns.poll();
		for(String token : tokens) {
			TokenReadHandler handler = readHandlers.get(token);
			if(handler != null) {
				handler.handle(in);
			}
		}
		
		payloadReader.readPayload(round, in, (ct)->{
			if(ct != null && ct.length > 0) {
				return symmetricState.decryptAndHash(ct);
			}
			
			return new byte[0];
		});
		
		if(messagePatterns.isEmpty()) return readyStates();
		return null;
	}
	
	protected CipherState[] readyStates() {
		// make sure that our rx state is element 0, tx is element 1
		CipherState[] states = symmetricState.split();
		
		localEphemeralKey.destroy();
		remoteEphemeralKey.destroy();
		
		if(isInitiator) return states;
		
		return new CipherState[] { states[1], states[0] };
	}
	
	private void initTokenMappers() {
		initWriteTokenMappers();
		initReadTokenMappers();
	}
	
	private void initWriteTokenMappers() {
		writeHandlers.put("e", (out)->{
			localEphemeralKey = generateKeypair();
			byte[] obfuscated = obfuscator.obfuscate(localEphemeralKey.publicKey());
			out.write(obfuscated);
			symmetricState.mixHash(obfuscated);
		});
		
		writeHandlers.put("s", (out)->{
			if(symmetricState.cipherState.hasKey()) {
				out.write(symmetricState.encryptAndHash(localStaticKey.publicKey().getBytes()));
			} else {
				byte[] obfuscated = obfuscator.obfuscate(localStaticKey.publicKey());
				out.write(symmetricState.encryptAndHash(obfuscated));
			}
		});
		
		writeHandlers.put("ee", (out)->{
			symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteEphemeralKey));
		});
		
		writeHandlers.put("es", (out)->{
			if(isInitiator) {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			} else {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			}
		});
		
		writeHandlers.put("se", (out)->{
			if(isInitiator) {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			} else {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			}
		});
		
		writeHandlers.put("ss", (out)->{
			symmetricState.mixKey(localStaticKey.sharedSecret(remoteStaticKey));
		});
		
		writeHandlers.put("psk", (out)->{
			symmetricState.mixKeyAndHash(psk);
		});
	}
	
	private void initReadTokenMappers() {
		readHandlers.put("e", (in)->{
			assert(remoteEphemeralKey == null);
			byte[][] deobfuscated = deobfuscator.deobfuscate(in);
			remoteEphemeralKey = new PublicDHKey(crypto, deobfuscated[0]);
			symmetricState.mixHash(deobfuscated[1]);
		});
		
		readHandlers.put("s", (in)->{
			assert(remoteStaticKey == null);
			
			if(symmetricState.cipherState.hasKey()) {
				// spec says dh key size + 16, where the 16 comes from the tag length
				int len = crypto.asymPublicDHKeySize() + crypto.symTagLength();
				byte[] temp = new byte[len];
				IOUtils.readFully(in, temp);
				remoteStaticKey = new PublicDHKey(crypto, symmetricState.decryptAndHash(temp));
			} else {
				byte[][] deobfuscated = deobfuscator.deobfuscate(in);
				remoteStaticKey = new PublicDHKey(crypto, deobfuscated[0]);
				symmetricState.mixHash(deobfuscated[1]);
			}
		});
		
		readHandlers.put("ee", (in)->{
			symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteEphemeralKey));
		});
		
		readHandlers.put("es", (in)->{
			if(isInitiator) {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			} else {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			}
		});
		
		readHandlers.put("se", (in)->{
			if(isInitiator) {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			} else {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			}
		});

		readHandlers.put("ss", (in)->{
			symmetricState.mixKey(localStaticKey.sharedSecret(remoteStaticKey));
		});
		
		readHandlers.put("psk", (in)->{
			symmetricState.mixKeyAndHash(psk);
		});
	}
	
	protected PrivateDHKey generateKeypair() {
		return new PrivateDHKey(crypto);
	}
	
	public String getProtocolName() {
		return protocolName;
	}
	
	public boolean isInitiator() {
		return isInitiator;
	}
	
	public byte[] getHash() {
		return symmetricState.getHandshakeHash();
	}

	public PublicDHKey getRemoteStaticKey() {
		return remoteStaticKey;
	}

	public void setPsk(byte[] psk) {
		this.psk = psk;
	}

	public PublicDHKey getLocalEphemeralKey() {
		return localEphemeralKey.publicKey();
	}
	
	public PublicDHKey getRemoteEphemeralKey() {
		return remoteEphemeralKey;
	}
}
