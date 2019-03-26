package com.acrescrypto.zksync.net.noise;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PrivateDHKey;
import com.acrescrypto.zksync.net.TCPPeerSocket;
import com.acrescrypto.zksync.utility.Util;

public class VariableLengthHandshakeStateTest {
	CryptoSupport crypto;
	VariableLengthHandshakeState init, resp;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() {
		crypto = CryptoSupport.defaultCrypto();
		
		PrivateDHKey initStaticKey = new PrivateDHKey(crypto);
		PrivateDHKey respStaticKey = new PrivateDHKey(crypto);
		
		byte[] prologue = "In the beginning".getBytes();
		byte[] psk = crypto.hash("shhh".getBytes());
		String protocolName = "Noise_XX_AndHeresSomeCryptoStuff";
		String messagePatterns = TCPPeerSocket.HANDSHAKE_PATTERN;
		
		init = new VariableLengthHandshakeState(crypto,
				protocolName,
				messagePatterns,
				true,
				prologue,
				initStaticKey,
				null,
				respStaticKey.publicKey(),
				null,
				psk);
		
		resp = new VariableLengthHandshakeState(crypto,
				protocolName,
				messagePatterns,
				false,
				prologue,
				respStaticKey,
				null,
				null,
				null,
				psk);
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	public void checkHandshake() {
		ServerSocket listener = null;
		try {
			listener = new ServerSocket(0);
			ServerSocket llistener = listener;
			CipherState[][] states = new CipherState[2][];
			
			Thread initThread = new Thread(()->{
				try {
					Socket s = new Socket("localhost", llistener.getLocalPort());
					states[0] = init.handshake(s.getInputStream(), s.getOutputStream());
					s.close();
				} catch(IOException exc) {
					fail();
				}
			});
			
			Thread respThread = new Thread(()->{
				try {
					Socket s = llistener.accept();
					states[1] = resp.handshake(s.getInputStream(), s.getOutputStream());
					s.close();
				} catch(IOException exc) {
					fail();
				}
			});
			
			initThread.start();
			respThread.start();
			
			assertTrue(Util.waitUntil(1000, ()->states[0] != null));
			assertTrue(Util.waitUntil(1000, ()->states[1] != null));
			
			assertEquals(states[0][0], states[1][1]);
			assertEquals(states[0][1], states[1][0]);
		} catch(IOException exc) {
			exc.printStackTrace();
			fail();
		} finally {
			if(listener != null) {
				try {
					listener.close();
				} catch (IOException exc) {
					exc.printStackTrace();
					fail();
				}
			}
		}
	}
	
	byte[] expectedNonce(int round) {
		return crypto.expand(crypto.symNonce(round), 1024*round, new byte[0], new byte[0]);
	}
	
	@Test
	public void testHandshakeWithoutHandlers() {
		checkHandshake();
	}
	
	@Test
	public void testHandshakeWithHandlers() {
		MutableInt initChecks = new MutableInt(), respChecks = new MutableInt();
		
		init.setSimplePayload(
			(round) -> expectedNonce(round),				
			(round, payload)->{
				initChecks.increment();
				assertArrayEquals(expectedNonce(round), payload);
			});
		resp.setSimplePayload(
			(round) -> expectedNonce(round),				
			(round, payload)->{
				respChecks.increment();
				assertArrayEquals(expectedNonce(round), payload);
			});
		
		checkHandshake();
		
		assertEquals(2, initChecks.intValue());
		assertEquals(2, respChecks.intValue());
	}
	
	@Test
	public void testHandshakeWithNullPayload() {
		MutableInt initChecks = new MutableInt(), respChecks = new MutableInt();
		
		init.setSimplePayload(
			(round) -> null,				
			(round, payload)->{
				initChecks.increment();
				assertEquals(null, payload);
			});
		resp.setSimplePayload(
			(round) -> null,				
			(round, payload)->{
				respChecks.increment();
				assertEquals(null, payload);
			});
		
		checkHandshake();
		
		assertEquals(2, initChecks.intValue());
		assertEquals(2, respChecks.intValue());
	}
	
	// TODO: test prehash
}
