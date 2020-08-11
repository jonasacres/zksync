package com.acrescrypto.zksyncweb.resources.archive.net;

import java.io.IOException;
import java.util.Base64;
import java.util.LinkedList;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.net.PeerConnection;
import com.acrescrypto.zksync.net.TCPPeerAdvertisement;
import com.acrescrypto.zksync.net.TCPPeerSocket;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.ArchiveCrud;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XPeerInfo;
import com.acrescrypto.zksyncweb.data.XTCPAdListing;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/archives/{archiveid}/net/peers")
public class ArchiveNetPeersResource {
	@GET
	public XAPIResponse getPeers(@PathParam("archiveid") String archiveId) throws IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();

		LinkedList<XPeerInfo> peers = new LinkedList<>();
		for(PeerConnection conn : config.getSwarm().getConnections()) {
			peers.add(new XPeerInfo(conn));
		}
		
		throw XAPIResponse.withWrappedPayload("peers", peers);
	}
	
	@PUT
	@Path("connect/tcp")
	public XAPIResponse putConnect(@PathParam("archiveid") String archiveId,
			String json) throws JsonParseException, JsonMappingException, IOException {
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();

		XTCPAdListing adl = new ObjectMapper().readValue(json, XTCPAdListing.class);
		PublicDHKey pubKey = new PublicDHKey(config.getCrypto(), adl.getPubKey());
		TCPPeerAdvertisement ad = new TCPPeerAdvertisement(pubKey,
				adl.getHost(),
				adl.getPort(),
				adl.getEncryptedArchiveId(),
				adl.getVersion());
		
		config.getSwarm().addPeerAdvertisement(ad);
		
		throw XAPIResponse.successResponse();
	}
	
	@GET
	@Path("{peerid:.*}")
	public XAPIResponse getPeer(@PathParam("archiveid") String archiveId,
			@PathParam("peerid") String fullPeerId) throws IOException {
		String peerId = ArchiveCrud.basePath(fullPeerId).substring(1);
		
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		PeerConnection conn = findConnection(config, peerId);
		
		throw XAPIResponse.withPayload(new XPeerInfo(conn));
	}
	
	@DELETE
	@Path("{peerid:.*}")
	public XAPIResponse deletePeer(@PathParam("archiveid") String archiveId,
			@PathParam("peerid") String fullPeerId,
			@Context UriInfo uriInfo) throws IOException {
		String peerId = ArchiveCrud.basePath(fullPeerId).substring(1);
		Map<String,String> params = ArchiveCrud.convertMultivaluedToSingle(uriInfo.getQueryParameters());
		long blacklistSecs = Long.parseLong(params.getOrDefault("blacklist", "-1")), blacklistMs;
		blacklistMs = blacklistSecs < 0 ? Long.MAX_VALUE : 1000*blacklistSecs;
		
		ZKArchiveConfig config = State.sharedState().configForArchiveId(archiveId);
		if(config == null) throw XAPIResponse.notFoundErrorResponse();
		
		PeerConnection conn = findConnection(config, peerId);
		if(params.containsKey("blacklist")) {
			// ?blacklist=1234 causes us to blacklist the peer's address for 1234 seconds
			// specify -1 to blacklist forever
			if(blacklistMs == Long.MAX_VALUE) {
				config.getMaster().getBlacklist().addWithAbsoluteTime(conn.getSocket().getAddress(), blacklistMs);
			} else {
				config.getMaster().getBlacklist().add(conn.getSocket().getAddress(), blacklistMs);
			}
		}
		conn.close();

		throw XAPIResponse.successResponse();
	}
	
	protected PeerConnection findConnection(ZKArchiveConfig config, String peerId) {
		peerId = Util.fromWebSafeBase64(peerId);
		for(PeerConnection conn : config.getSwarm().getConnections()) {
			if(conn.getSocket() instanceof TCPPeerSocket) {
				TCPPeerSocket socket = (TCPPeerSocket) conn.getSocket();
				String b64 = Base64.getEncoder().encodeToString(socket.getAd().getPubKey().getBytes());
				if(b64.startsWith(peerId)) {
					return conn;
				}
			}
		}
		
		throw XAPIResponse.notFoundErrorResponse();
	}
}
