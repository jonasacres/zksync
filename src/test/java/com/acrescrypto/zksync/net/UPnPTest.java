package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.utility.Util;
import com.dosse.upnp.UPnP;

public class UPnPTest {
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
		UPnP.disableDebug();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	@Test @Ignore
	public void debugUPnPAvailability() {
		System.out.println("UPnP Available: " + UPnP.isUPnPAvailable());
		System.out.println("      Local IP: " + UPnP.getLocalIP());
		System.out.println("   External IP: " + UPnP.getExternalIP());
	}
	
	@Test
	public void testTcpPortMapping() {
		if(!UPnP.isUPnPAvailable()) {
			// can't test UPnP if it is not available
			return;
		}
		
		int testPort = 59477;
		if(UPnP.isMappedTCP(testPort)) {
			UPnP.closePortTCP(testPort);
			assertTrue(Util.waitUntil(1000, ()->UPnP.isMappedTCP(testPort) == false));
		}
		
		UPnP.openPortTCP(testPort);
		assertTrue(Util.waitUntil(1000, ()->UPnP.isMappedTCP(testPort)));

		UPnP.closePortTCP(testPort);
		assertTrue(Util.waitUntil(1000, ()->UPnP.isMappedTCP(testPort) == false));
	}
	
	@Test
	public void testUdpPortMapping() {
		if(!UPnP.isUPnPAvailable()) {
			// can't test UPnP if it is not available
			return;
		}

		int testPort = 59477;
		if(UPnP.isMappedUDP(testPort)) {
			UPnP.closePortUDP(testPort);
			assertTrue(Util.waitUntil(1000, ()->UPnP.isMappedUDP(testPort) == false));
		}
		
		UPnP.openPortUDP(testPort);
		assertTrue(Util.waitUntil(1000, ()->UPnP.isMappedUDP(testPort)));

		UPnP.closePortUDP(testPort);
		assertTrue(Util.waitUntil(1000, ()->UPnP.isMappedUDP(testPort) == false));
	}
}
