package com.acrescrypto.zksyncweb.resources.dht;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriInfo;

import com.acrescrypto.zksync.net.dht.DHTBootstrapper;
import com.acrescrypto.zksync.net.dht.DHTClient;
import com.acrescrypto.zksync.net.dht.DHTID;
import com.acrescrypto.zksync.net.dht.DHTPeer;
import com.acrescrypto.zksync.net.dht.DHTRecordStore.StoreEntry;
import com.acrescrypto.zksyncweb.State;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XDHTInfo;
import com.acrescrypto.zksyncweb.data.XDHTPeerFile;
import com.acrescrypto.zksyncweb.data.XDHTPeerInfo;
import com.acrescrypto.zksyncweb.data.XDHTRecord;
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
	@Path("peerfile")
	public XAPIResponse getDhtPeerFile() throws IOException {
		XDHTPeerFile peerFile = new XDHTPeerFile(State.sharedState().getMaster().getDHTClient()); 
		throw XAPIResponse.withPayload(peerFile);
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
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("peers")
	public XAPIResponse deleteDhtPeers() throws IOException {
		DHTClient client = State.sharedState().getMaster().getDHTClient();
		client.getRoutingTable().reset();
		client.write();
		throw XAPIResponse.successResponse();
	}
	
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("records")
	public XAPIResponse getDhtRecords() throws IOException {
		HashMap<String, LinkedList<XDHTRecord>> outRecords    = new HashMap<>();
		Map<DHTID, Collection<StoreEntry>>      sourceRecords = State
				 												.sharedState()
				 												.getMaster()
				 												.getDHTClient()
				 												.getRecordStore()
				 												.records();
		sourceRecords.forEach((id, records)->{
			LinkedList<XDHTRecord> list = new LinkedList<>();
			records.forEach((record)->{
				list.add(new XDHTRecord(record));
			});
			
			outRecords.put(id.toFullString(), list);
		});
		
		throw XAPIResponse.withWrappedPayload("records", outRecords);
	}
	
	@DELETE
	@Produces(MediaType.APPLICATION_JSON)
	@Path("records")
	public XAPIResponse deleteDhtRecords() throws IOException {
		DHTClient client = State.sharedState().getMaster().getDHTClient();
		client.getRecordStore().reset();
		client.write();
		throw XAPIResponse.successResponse();
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
	@Path("bootstrap")
	public XAPIResponse postBootstrap(@Context UriInfo uriInfo, byte[] contents) throws IOException {
		DHTBootstrapper bootstrapper = State.sharedState().getMaster().getDHTClient().bootstrapper();
		if(contents.length == 0) {
			bootstrapper.bootstrap();
		} else {
			bootstrapper.bootstrapFromPeerFileString(new String(contents));
		}
		
		throw XAPIResponse.successResponse();
	}
	
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path("refresh")
	public XAPIResponse postRefresh() throws IOException {
		State.sharedState()
			 .getMaster()
			 .getDHTClient()
			 .pingAll();
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
