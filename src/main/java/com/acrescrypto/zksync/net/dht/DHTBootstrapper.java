package com.acrescrypto.zksync.net.dht;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigFile;
import com.acrescrypto.zksyncweb.ObjectMapperProvider;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DHTBootstrapper {
	protected DHTClient client;
	
	private Logger logger = LoggerFactory.getLogger(DHTBootstrapper.class);
	
	public DHTBootstrapper(DHTClient client) {
		this.client = client;
	}
	
	public void bootstrap() throws JsonProcessingException, IOException {
		ConfigFile cfg  = client.getMaster().getGlobalConfig();
		String peerFile = cfg.getString("net.dht.bootstrap.peerfile");
		
		if(peerFile == null || peerFile.isEmpty())     return;
		if(!cfg.getBool("net.dht.bootstrap.enabled"))  return;
		
		bootstrapFromPeerFileString(peerFile);
	}
	
	public void bootstrapFromPeerFileString(String pfString) throws JsonProcessingException, IOException {
		ObjectMapper mapper = new ObjectMapperProvider().getContext(null);
		JsonNode     pf     = null;
		String       addr   = null;
		
		try {
			URL      urlObj = new URL(pfString);
			if(urlObj.getProtocol().equals("file")) {
				logger.info("DHTBootstrapper: Bootstrapping from local file {}", urlObj.getPath());
				         pf = mapper.readTree(new File(urlObj.getPath()));
			} else {
				logger.info("DHTBootstrapper: Bootstrapping from URL {}", pfString);
				       addr = urlObj.getHost();
				         pf = mapper.readTree(urlObj);
				logger.info("DHTBootstrapper: Received peerfile JSON from remote host");
			}
		} catch(MalformedURLException exc) {
			pf = mapper.readTree(pfString);
			logger.info("DHTBootstrapper: Bootstrapping from supplied peerfile, {} bytes", pfString.length());
		}
		
		if(pf.has("resp") && pf.get("resp").isObject() && !pf.has("networkId")) {
			pf = pf.get("resp");
		}
		
		validatePeerFile(pf);
		bootstrapFromPeerFile(pf, addr);
	}
	
	public void bootstrapFromPeerFile(JsonNode pf, String remoteAddress) throws JsonProcessingException, IOException {
		client.setNetworkId(pf.get("networkId").binaryValue());
		
		pf.get("peers").forEach((entry) -> {
			try {
				byte[]      pubKey    = entry.get("pubKey").binaryValue();
				String      address   = entry.get("address").asText();
				int         port      = entry.get("port").asInt();
				
				if(address.equals("0.0.0.0")) {
					if(remoteAddress == null) return;
					address = remoteAddress;
				}
				
				PublicDHKey key       = new PublicDHKey(client.getMaster().getCrypto(), pubKey);
				DHTPeer     peer      = new DHTPeer    (client, address, port, key);
				
				client.getRoutingTable().suggestPeer(peer);
			} catch(IOException exc) {
				logger.error("DHTBootstrapper: Error parsing peerfile", exc);
			}
		});
		
		logger.info("DHTBootstrapper: Initialized {} DHT entries from bootstrap",
				pf.get("peers").size());
	}
	
	protected void validatePeerFile(JsonNode pf) throws IOException {
		if(!pf.has("networkId")) throw new InvalidPeerFileException("Expected 'networkId' field");
		if(!pf.has("peers"))     throw new InvalidPeerFileException("Expected 'peers' field");
		
		// if(!pf.get("networkId").isBinary()) throw new InvalidPeerFileException("Expected 'networkId' field to be binary");
		if(!pf.get("peers").isArray())      throw new InvalidPeerFileException("Expected 'peers' field to be array");
		
		byte[] networkId  = pf.get("networkId").binaryValue();
		int    hashLength = client.getMaster().getCrypto().hashLength();
		if(networkId.length != hashLength) {
			throw new InvalidPeerFileException("Expected 'networkId' to have length " + hashLength + "; is " + hashLength + " bytes");
		}
		
		Iterator<JsonNode> itr = pf.get("peers").iterator();
		int index = 0;
		
		while(itr.hasNext()) {
			JsonNode peer = itr.next();
			if(!peer.isObject())     throw new InvalidPeerFileException("Expected peers[" + index + "] to be object");

			if(!peer.has("pubKey"))  throw new InvalidPeerFileException("Expected peers[" + index + "] to have 'pubKey' field");
			if(!peer.has("port"))    throw new InvalidPeerFileException("Expected peers[" + index + "] to have 'port' field");
			if(!peer.has("address")) throw new InvalidPeerFileException("Expected peers[" + index + "] to have 'address' field");
			
			if(!peer.get("pubKey").isTextual())   throw new InvalidPeerFileException("Expected peers[" + index + "].pubKey to be binary");
			if(!peer.get("port").isInt())         throw new InvalidPeerFileException("Expected peers[" + index + "].pubKey to be integer");
			if(!peer.get("address").isTextual())  throw new InvalidPeerFileException("Expected peers[" + index + "].pubKey to be string");
			
			int    port    = peer.get("port")   .asInt();
			byte[] pubKey  = peer.get("pubKey") .binaryValue();
			
			if(port <= 0 || port > 65535) {
				throw new InvalidPeerFileException("Expected peers[" + index + "] to contain legal UDP port in range [1, 65535]; has value " + port);
			}
			
			int pubKeyLen = client.getMaster().getCrypto().asymPublicDHKeySize();
			if(pubKey.length != pubKeyLen) {
				throw new InvalidPeerFileException("Expected peers[" + index + "] to contain public key of length " + pubKeyLen + "; is " + pubKey.length + " bytes");
			}
			
			// TODO: validate address? harder to do, since it can be IPv4, IPv6 or domain
			
			index++;
		};
	}
}