/*
 * Copyright (C) 2015 Federico Dossena (adolfintel.com).
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA 02110-1301  USA
 */

/*
 * Minor adjustments have been made to support testing for EasySafe.
 */
package com.dosse.upnp;

import java.util.concurrent.ConcurrentHashMap;

/**
 * This class contains static methods that allow quick access to UPnP Port Mapping.<br>
 * Commands will be sent to the default gateway.
 * 
 * @author Federico
 */
public class UPnP {
	private static boolean debug = false;
	private static ConcurrentHashMap<Integer,Boolean> tcpMappings = new ConcurrentHashMap<>();
	private static ConcurrentHashMap<Integer,Boolean> udpMappings = new ConcurrentHashMap<>();
	private static Gateway defaultGW = null;

	public static void enableDebug() {
		debug = true;
	}
	
	public static void disableDebug() {
		debug = false;
		tcpMappings.clear();
		udpMappings.clear();
	}	
	
	private static final GatewayFinder finder = new GatewayFinder() {
		@Override
		public void gatewayFound(Gateway g) {
			synchronized (finder) {
				if (defaultGW == null) {
					defaultGW = g;
				}
			}
		}
	};

	/**
	 * Waits for UPnP to be initialized (takes ~3 seconds).<br>
	 * It is not necessary to call this method manually before using UPnP functions
	 */
	public static void waitInit() {
		while (finder.isSearching()) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException ex) {
			}
		}
	}

	/**
	 * Is there an UPnP gateway?<br>
	 * This method is blocking if UPnP is still initializing<br>
	 * All UPnP commands will fail if UPnP is not available
	 * 
	 * @return true if available, false if not
	 */
	public static boolean isUPnPAvailable(){
		if(debug) return true;
		waitInit();
		return defaultGW!=null;
	}

	/**
	 * Opens a TCP port on the gateway
	 * 
	 * @param port TCP port (0-65535)
	 * @return true if the operation was successful, false otherwise
	 */
	public static boolean openPortTCP(int port) {
		if(!isUPnPAvailable()) return false;
		if(debug) {
			tcpMappings.put(port, true);
			return true;
		}

		return defaultGW.openPort(port, false);
	}

	/**
	 * Opens a UDP port on the gateway
	 * 
	 * @param port UDP port (0-65535)
	 * @return true if the operation was successful, false otherwise
	 */
	public static boolean openPortUDP(int port) {
		if(!isUPnPAvailable()) return false;
		if(debug) {
			udpMappings.put(port, true);
			return true;
		}

		return defaultGW.openPort(port, true);
	}

	/**
	 * Closes a TCP port on the gateway<br>
	 * Most gateways seem to refuse to do this
	 * 
	 * @param port TCP port (0-65535)
	 * @return true if the operation was successful, false otherwise
	 */
	public static boolean closePortTCP(int port) {
		if(!isUPnPAvailable()) return false;
		if(debug) {
			tcpMappings.remove(port);
			return true;
		}

		return defaultGW.closePort(port, false);
	}

	/**
	 * Closes a UDP port on the gateway<br>
	 * Most gateways seem to refuse to do this
	 * 
	 * @param port UDP port (0-65535)
	 * @return true if the operation was successful, false otherwise
	 */
	public static boolean closePortUDP(int port) {
		if(!isUPnPAvailable()) return false;
		if(debug) {
			udpMappings.remove(port);
			return true;
		}
		
		return defaultGW.closePort(port, true);
	}

	/**
	 * Checks if a TCP port is mapped<br>
	 * 
	 * @param port TCP port (0-65535)
	 * @return true if the port is mapped, false otherwise
	 */
	public static boolean isMappedTCP(int port) {
		if(!isUPnPAvailable()) return false;
		if(debug) return tcpMappings.getOrDefault(port, false);
		return defaultGW.isMapped(port, false);
	}

	/**
	 * Checks if a UDP port is mapped<br>
	 * 
	 * @param port UDP port (0-65535)
	 * @return true if the port is mapped, false otherwise
	 */
	public static boolean isMappedUDP(int port) {
		if(!isUPnPAvailable()) return false;
		if(debug) return udpMappings.getOrDefault(port, false);
		
		return defaultGW.isMapped(port, true);
	}

	/**
	 * Gets the external IP address of the default gateway
	 * 
	 * @return external IP address as string, or null if not available
	 */
	public static String getExternalIP(){
		if(!isUPnPAvailable()) return null;
		if(debug) return "0.0.0.0";
		return defaultGW.getExternalIP();
	}

	/**
	 * Gets the internal IP address of this machine
	 * 
	 * @return internal IP address as string, or null if not available
	 */
	public static String getLocalIP(){
		if(!isUPnPAvailable()) return null;
		if(debug) return "127.0.0.1";
		return defaultGW.getLocalIP();
	}
}
