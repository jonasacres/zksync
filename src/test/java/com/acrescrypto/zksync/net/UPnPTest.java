package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.acrescrypto.zksync.utility.Util;
import com.dosse.upnp.UPnP;

public class UPnPTest {
	@Test
	public void testTcpPortMapping() {
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
