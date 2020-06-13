package com.acrescrypto.zksync.net.noise;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.net.noise.SymmetricState.DerivationCallback;
import com.acrescrypto.zksync.utility.channeldispatcher.ChannelDispatchMonitor;

// per section 5.3, Noise specification, http://noiseprotocol.org/noise.html, rev 34, 2018-07-11
public class HandshakeState {
	protected CryptoSupport                       crypto;
	protected SymmetricState                      symmetricState;
	
	protected PrivateDHKey                        localStaticKey;       //  's' in noise spec
	protected PrivateDHKey                        localEphemeralKey;    //  'e' in noise spec
	protected PublicDHKey                         remoteStaticKey;      // 'rs' in noise spec
	protected PublicDHKey                         remoteEphemeralKey;   // 're' in noise spec
	
	protected boolean                             isInitiator;          // 'initiator' in noise spec
	protected Queue    <TokenHandler>             tokenHandlers;        // analogous to 'message_patterns' in noise spec
	
	protected HashMap  <String,TokenHandler>      writeHandlers = new HashMap<>();
	protected HashMap  <String,TokenHandler>      readHandlers  = new HashMap<>();
	
	protected byte[]                              psk;
	
	protected String                              protocolName;
	
	protected KeyDeobfuscator                     deobfuscator;
	protected KeyObfuscator                       obfuscator;
	protected PayloadReader                       payloadReader;
	protected PayloadWriter                       payloadWriter;
	
	protected HandshakeCompletionCallback         handshakeCallback;
	protected ExceptionHandlerCallback            exceptionHandler;
	
	protected int round;
	
	private interface TokenHandler {
		public boolean handle(ChannelDispatchMonitor peer) throws IOException;
	}
	
	public interface DecryptHandler {
		public byte[] decrypt(byte[] ciphertext);
	}
	
	public interface PayloadReader {
		/** Callback for handling additional payload in handshaking.
		 * 
		 * @param round Message round. The first message from the initiator is round 1, the response is round 2, the next initiator message is round 3, and so on.
		 * @param peer ChannelDispatchMonitor for peer from which to receive ciphertext
		 * @param decrypter DecryptHandler for decrypting ciphertext. Returns plaintext. Must be called exactly once on a byte array containing entire ciphertext read from peer, or handshaking will fail.
		 * @return Ciphertext bytes read from peer.
		 * @throws IOException
		 */
		public void readPayload(int round, ChannelDispatchMonitor peer, DecryptHandler decrypter) throws IOException;
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
		public void deobfuscate(ChannelDispatchMonitor peer, KeyDeobfuscatorCallback callback) throws IOException;
	}
	
	public interface KeyDeobfuscatorCallback {
		public void deobfuscated(byte[] plaintextKey, byte[] obfuscatedKey);
	}
	
	public interface HandshakeCompletionCallback {
		public void complete(CipherState rx, CipherState tx) throws Exception;
	}
	
	public interface ExceptionHandlerCallback {
		public void exception(Exception exc);
	}

	public HandshakeState(
			CryptoSupport crypto,
			String        protocolName,
			String        handshakePattern,
			boolean       isInitiator,
			byte[]        prologue,
			PrivateDHKey  localStaticKey,
			PrivateDHKey  localEphemeralKey,
			PublicDHKey   remoteStaticKey,
			PublicDHKey   remoteEphemeralKey,
			byte[]        psk) {
		initTokenMappers();
		
		this.crypto         = crypto;
		this.protocolName   = protocolName;
		this.symmetricState = new SymmetricState(crypto, protocolName);
		
		this.deobfuscator = (peer, callback) -> {
			peer.expect(crypto.asymPublicDHKeySize(), (key)->{
				callback.deobfuscated(key.array(), key.array());
			});
		};
		
		this.obfuscator = (key) -> key.getBytes();
		
		this.payloadReader = (round, peer, decrypter) -> decrypter.decrypt(new byte[0]);
		this.payloadWriter = (round) -> new byte[0];
		
		if(prologue == null) prologue = new byte[0];
		this.symmetricState.mixHash(prologue);

		this.isInitiator        = isInitiator;
		this.localStaticKey     = localStaticKey;
		this.localEphemeralKey  = localEphemeralKey;
		this.remoteStaticKey    = remoteStaticKey;
		this.remoteEphemeralKey = remoteEphemeralKey;
		this.psk = psk;
		
		decodeMessagePattern(handshakePattern);
	}
	
	public void setObfuscation(KeyObfuscator obfuscator, KeyDeobfuscator deobfuscator) {
		this.obfuscator    = obfuscator;
		this.deobfuscator  = deobfuscator;
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
	
	public void setDerivationCallback(DerivationCallback derivationCallback) {
		symmetricState.setDerivationCallback(derivationCallback);
	}
	
	public void setExceptionHandler(ExceptionHandlerCallback exceptionHandler) {
		this.exceptionHandler = exceptionHandler;
	}

	protected void decodeMessagePattern(String bigPattern) {
		this.tokenHandlers = new LinkedList<>();
		
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
				boolean shouldRead = lineForInitiator == isInitiator; 
				for(String token : line.split(",? ")) {
					TokenHandler handler = shouldRead
							             ?  readHandlers.get(token)
							             : writeHandlers.get(token);
					tokenHandlers.add(handler);
				}
				
				tokenHandlers.add(shouldRead
						? (peer) -> handleReadPayload (peer)
						: (peer) -> handleWritePayload(peer)
					);
			}
		}
	}
	
	public void handshake(ChannelDispatchMonitor peer, HandshakeCompletionCallback callback) {
		this.handshakeCallback = callback;
		resumeHandshake(peer);
	}
	
	protected void resumeHandshake(ChannelDispatchMonitor peer) {
		boolean             proceed = true;
		
		try {
			while(proceed) {
				if(tokenHandlers.isEmpty()) {
					// we're done!!
					CipherState[] states = readyStates(); // [rx, tx]
					handshakeCallback.complete(states[0], states[1]);
					return;
				}
					
				TokenHandler handler = tokenHandlers.poll();
				proceed              = handler.handle(peer);
			}
		} catch(Exception exc) {
			if(exceptionHandler != null) {
				exceptionHandler.exception(exc);
			}
		}
	}
	
	protected boolean handleWritePayload(ChannelDispatchMonitor peer) throws IOException {
		byte[] payload = payloadWriter.writePayload(round);
		if(payload != null && payload.length > 0) {
			byte[] ciphertext = symmetricState.encryptAndHash(payload);
			peer.send(ciphertext);
		}
		
		return true;
	}
	
	protected boolean handleReadPayload(ChannelDispatchMonitor peer) throws IOException {
		payloadReader.readPayload(round, peer, (ct)->{
			if(ct != null && ct.length > 0) {
				return symmetricState.decryptAndHash(ct);
			}
			
			return new byte[0];
		});
		
		return false;
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
		writeHandlers.put("e", (peer)->{
			localEphemeralKey = generateKeypair();
			byte[] obfuscated = obfuscator.obfuscate(localEphemeralKey.publicKey());
			peer.send(obfuscated);
			symmetricState.mixHash(obfuscated);
			
			return true;
		});
		
		writeHandlers.put("s", (peer)->{
			if(symmetricState.cipherState.hasKey()) {
				peer.send(symmetricState.encryptAndHash(localStaticKey.publicKey().getBytes()));
			} else {
				byte[] obfuscated = obfuscator.obfuscate(localStaticKey.publicKey());
				peer.send(symmetricState.encryptAndHash(obfuscated));
			}
			
			return true;
		});
		
		writeHandlers.put("ee", (out)->{
			symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteEphemeralKey));
			return true;
		});
		
		writeHandlers.put("es", (out)->{
			if(isInitiator) {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			} else {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			}
			return true;
		});
		
		writeHandlers.put("se", (out)->{
			if(isInitiator) {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			} else {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			}
			return true;
		});
		
		writeHandlers.put("ss", (out)->{
			symmetricState.mixKey(localStaticKey.sharedSecret(remoteStaticKey));
			return true;
		});
		
		writeHandlers.put("psk", (out)->{
			symmetricState.mixKeyAndHash(psk);
			return true;
		});
	}
	
	private void initReadTokenMappers() {
		readHandlers.put("e", (peer)->{
			assert(remoteEphemeralKey == null);
			deobfuscator.deobfuscate(peer, (plaintextKey, obfuscatedKey)->{
				remoteEphemeralKey = new PublicDHKey(crypto, plaintextKey);
				symmetricState.mixHash(obfuscatedKey);
				resumeHandshake(peer);
			});
			
			return false;
		});
		
		readHandlers.put("s", (peer)->{
			assert(remoteStaticKey == null);
			
			if(symmetricState.cipherState.hasKey()) {
				// spec says dh key size + 16, where the 16 comes from the tag length
				int len = crypto.asymPublicDHKeySize() + crypto.symTagLength();
				peer.expect(len, (bytes)->{
					remoteStaticKey = new PublicDHKey(crypto, symmetricState.decryptAndHash(bytes.array()));
					resumeHandshake(peer);
				});
			} else {
				deobfuscator.deobfuscate(peer, (plaintextKey, obfuscatedKey)->{
					remoteStaticKey = new PublicDHKey(crypto, plaintextKey);
					symmetricState.mixHash(obfuscatedKey);
					resumeHandshake(peer);
				});
			}
			
			return false;
		});
		
		readHandlers.put("ee", (in)->{
			symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteEphemeralKey));
			return true;
		});
		
		readHandlers.put("es", (in)->{
			if(isInitiator) {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			} else {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			}
			
			return true;
		});
		
		readHandlers.put("se", (in)->{
			if(isInitiator) {
				symmetricState.mixKey(localStaticKey.sharedSecret(remoteEphemeralKey));
			} else {
				symmetricState.mixKey(localEphemeralKey.sharedSecret(remoteStaticKey));
			}
			
			return true;
		});

		readHandlers.put("ss", (in)->{
			symmetricState.mixKey(localStaticKey.sharedSecret(remoteStaticKey));
			return true;
		});
		
		readHandlers.put("psk", (in)->{
			symmetricState.mixKeyAndHash(psk);
			return true;
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
