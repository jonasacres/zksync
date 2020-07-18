package com.acrescrypto.zksync.net.dht;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.dosse.upnp.UPnP;

public class DHTSocketManager {
	protected DHTClient           client;
	protected BandwidthMonitor    monitorTx,
	                              monitorRx;
	protected DatagramSocket      socket;
	protected String              bindAddress;
	protected Thread              socketListenerThread;
	protected boolean             paused;
	protected int                 bindPort;

	private Logger logger = LoggerFactory.getLogger(DHTSocketManager.class);

	public DHTSocketManager(DHTClient client) {
		this.client = client;
		
		if(client.getMaster() != null) {
			this.monitorTx = new BandwidthMonitor(client.getMaster().getBandwidthMonitorTx());
			this.monitorRx = new BandwidthMonitor(client.getMaster().getBandwidthMonitorRx());
		}
	}
	
	protected DHTSocketManager() {}
	
	public void listen(String address, int port) throws SocketException {
		if(port == 0) {
			bindPort = client.getMaster().getGlobalConfig().getInt("net.dht.lastport");
		} else {
			bindPort = port;
		}
		
		this.paused      = false;
		this.bindAddress = address == null
				? client.getMaster().getGlobalConfig().getString("net.dht.bindaddress")
				: address;
				
		openSocket();

		if(socketListenerThread == null || !socketListenerThread.isAlive()) {
			socketListenerThread = new Thread(client.getThreadGroup(), ()->socketListener());
			socketListenerThread.start();
		}
	}
	
	public void pause() {
		paused   = true;
		int port = getPort();
		
		if(socket != null) {
			socket.close();
		}

		if(port > 0 && client.getMaster().getGlobalConfig().getBool("net.dht.upnp")) {
			UPnP.closePortUDP(port);
		}
	}

	public String getBindAddress() {
		return bindAddress;
	}
	
	public int getPort() {
		if(socket == null) return -1;
		return socket.getLocalPort();
	}
	
	public void setBindPort(int bindPort) {
		this.bindPort = bindPort;
	}
	
	public void setBindAddress(String bindAddress) {
		this.bindAddress = bindAddress;
	}
	
	public boolean isPaused() {
		return paused;
	}
	
	public boolean isListening() {
		return !paused && socket != null;
	}
	
	
	
	protected void openSocket() throws SocketException {
		if(paused) return;
		
		InetAddress addr;
		try {
			addr = InetAddress.getByName(bindAddress);
		} catch(UnknownHostException exc) {
			throw new RuntimeException("Unable to bind to address " + bindAddress + ":" + bindPort + ": unable to resolve address");
		}
		
		client.updateStatus(DHTClient.STATUS_ESTABLISHING);
		
		if(socket != null && !socket.isClosed()) {
			int oldPort = socket.getLocalPort();
			socket.close();
			if(oldPort != bindPort && client.getMaster().getGlobalConfig().getBool("net.dht.upnp")) {
				logger.info("DHT -: Closing UPnP for DHT on UDP port " + getPort());
				UPnP.closePortUDP(oldPort);
			}
		}
		
		while(true) {
			try {
				socket = new DatagramSocket(bindPort, addr);
				socket.setReuseAddress(true);
				client.getMaster().getGlobalConfig().set("net.dht.lastport", socket.getLocalPort());;
				
				logger.info("DHT -: listening on UDP port " + getPort());
				
				checkUPnP();
				client.updateStatus(DHTClient.STATUS_QUESTIONABLE);
				break;
			} catch(SocketException exc) {
				if(client.getMaster().getGlobalConfig().getInt("net.dht.port") == 0 && bindPort != 0) {
					bindPort = 0;
				} else {
					client.updateStatus(DHTClient.STATUS_OFFLINE);
					throw exc;
				}
			}
		}
	}
	
	protected void checkUPnP() {
		boolean useUPnP = client.getMaster().getGlobalConfig().getBool("net.dht.upnp");
		
		if(useUPnP && socket != null && !paused) {
			logger.info("DHT -: Requesting UPnP for DHT on UDP port " + getPort());
			UPnP.openPortUDP(getPort());
		}
	}
	
	protected void setUPnPEnabled(boolean enabled) {
		if(enabled) {
			checkUPnP();
		} else if(getPort() > 0 && UPnP.isMappedUDP(getPort())) {
			UPnP.closePortTCP(getPort());
		}
	}
	
	protected void socketListener() {
		Util.setThreadName("DHTSocketManager socketListener " + Util.bytesToHex(client.getPublicKey().getBytes(), 4) + " " + getPort());
		int lastPort = -1;
		
		while(!paused) {
			try {
				if(socket == null) {
					System.out.println("Waiting for socket to open");
					Util.sleep(10);
					continue;
				}
				
				lastPort = socket.getLocalPort();
				
				int maxDatagramSize = client.getMaster().getGlobalConfig().getInt("net.dht.maxDatagramSize");
				byte[] receiveData = new byte[maxDatagramSize];
				DatagramPacket packet = new DatagramPacket(receiveData, receiveData.length);
				socket.receive(packet);
				monitorRx.observeTraffic(packet.getLength());
				logger.trace("DHT {}:{}: received {} bytes",
						packet.getAddress().getHostAddress(),
						packet.getPort(),
						packet.getLength());
				ByteBuffer buf = ByteBuffer.wrap(packet.getData(), 0, packet.getLength());
				client.getProtocolManager().processMessage(
						packet.getAddress().getHostAddress(),
						packet.getPort(),
						buf
					);
			} catch(IOException exc) {
				if(paused) return;
				if(socket.getLocalPort() == lastPort && !socket.isClosed()) {
					logger.error("DHT -: socket listener thread encountered IOException", exc);
				} else {
					logger.warn("DHT -: socket listener thread encountered IOException", exc);
				}
				
				int socketCycleDelayMs         = client.getMaster().getGlobalConfig().getInt("net.dht.socketCycleDelayMs"),
					socketOpenFailCycleDelayMs = client.getMaster().getGlobalConfig().getInt("net.dht.socketOpenFailCycleDelayMs");
				Util.sleep(socketCycleDelayMs); // add in a delay to prevent a fail loop from gobbling CPU / spamming log
				try {
					openSocket();
				} catch (SocketException e) {
					logger.error("DHT -: socket listener thread encountered IOException rebinding socket", exc);
					Util.sleep(socketOpenFailCycleDelayMs); // wait even longer if we know the socket is dead and the OS isn't giving it back
				}
			} catch(Exception exc) {
				if(socket.isClosed()) {
					logger.warn("DHT -: socket listener thread encountered exception", exc);
				} else {
					logger.error("DHT -: socket listener thread encountered exception", exc);
				}
			}
		}
	}
	
	protected synchronized void sendDatagram(DatagramPacket packet) {
		if(paused) return;
		for(int i = 0; i < 2; i++) {
			try {
				socket.send(packet);
				monitorTx.observeTraffic(packet.getLength());
				break;
			} catch (IOException exc) {
				// TODO API: (coverage) exception
				if(paused) return;
				exc.printStackTrace();
				if(i == 0) {
					logger.warn("DHT {}:{}: Encountered exception sending on DHT socket; retrying",
							packet.getAddress().getHostAddress(),
							packet.getPort(),
							exc);
					try {
						openSocket();
					} catch (SocketException exc2) {
						logger.error("DHT -: Encountered exception rebinding DHT socket; giving up on sending message", exc2);
						return;
					}
				} else {
					logger.error("DHT -: Encountered exception sending on DHT socket; giving up", exc);
				}
			}
		}
	}
	
	public BandwidthMonitor getMonitorRx() {
		return monitorRx;
	}
	
	public BandwidthMonitor getMonitorTx() {
		return monitorTx;
	}

	public void setMonitorRx(BandwidthMonitor monitorRx) {
		this.monitorRx = monitorRx;
	}
	
	public void setMonitorTx(BandwidthMonitor monitorTx) {
		this.monitorTx = monitorTx;
	}

	public String getLocalAddress() {
		return client.getMaster().getGlobalConfig().getString("net.dht.localaddress");
	}
	
	public String getSocketAddress() {
		try {
			InetSocketAddress addr = (InetSocketAddress) socket.getLocalSocketAddress();
			return addr.getAddress().getHostAddress();
		} catch(Exception exc) {
			return getLocalAddress();
		}
	}
}
