package com.acrescrypto.zksyncweb.resources.dht;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XDHTInfo;
import com.acrescrypto.zksyncweb.data.XDHTPeerInfo;
import com.fasterxml.jackson.databind.ObjectMapper;

@Path("/dht")
public class DHTResource {
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public XAPIResponse getDht() throws IOException {
		throw XAPIResponse.withPayload(new XDHTInfo(State.sharedState().getMaster().getDHTClient()));
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("peers")
	public XAPIResponse getDhtPeers() throws IOException {
		LinkedList<XDHTPeerInfo> peers = new LinkedList<>();
		Collection<DHTPeer> allPeers = State.sharedState().getMaster().getDHTClient().getRoutingTable().allPeers();
		for(DHTPeer peer : allPeers) {
			peers.add(new XDHTPeerInfo(peer));
		}
		
		throw XAPIResponse.withWrappedPayload("peers", peers);
	}
	
	@PUT
	@Produces(MediaType.APPLICATION_JSON)
	@Path("peers")
	public XAPIResponse putDHTPeer(String json) throws IOException {
		XDHTPeerInfo info = new ObjectMapper().readValue(json, XDHTPeerInfo.class);
		DHTClient client = State.sharedState().getMaster().getDHTClient();
		DHTPeer peer = new DHTPeer(client,
				info.getAddress(),
				info.getPort(),
				info.getPubKey());
		client.addPeer(peer);
		
		throw XAPIResponse.successResponse();
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("refresh")
	public XAPIResponse postRefresh() throws IOException {
		State.sharedState().getMaster().getDHTClient().pingAll();
		throw XAPIResponse.successResponse();
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("regenerate")
	public XAPIResponse postRegenerate() throws IOException {
		State.sharedState().getMaster().regenerateDHTClient();
		throw XAPIResponse.successResponse();
	}
}
