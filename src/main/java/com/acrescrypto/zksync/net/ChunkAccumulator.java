package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.utility.Util;

public class ChunkAccumulator {
	protected class PeerChunkInfo {
		PeerConnection peer;
		ChunkVersion version;
		
		public PeerChunkInfo(ChunkVersion version, PeerConnection peer) {
			this.version = version;
			this.peer = peer;
		}
	}
	
	protected class ChunkVersion {
		FS scratchFS;
		byte[] hash;
		
		public ChunkVersion(FS scratchFS, byte[] chunk) throws IOException {
			this.scratchFS = scratchFS;
			this.hash = swarm.config.getAccessor().getMaster().getCrypto().hash(chunk);
			write(chunk);
		}
		
		public byte[] getBytes() throws IOException {
			return scratchFS.read(path());
		}
		
		public void finish() throws IOException {
			scratchFS.unlink(path());
		}
		
		protected String path() {
			return Util.bytesToHex(hash);
		}
		
		protected void write(byte[] chunk) throws IOException {
			scratchFS.write(path(), chunk);
		}
	}
	
	protected byte[] tag;
	protected int numChunksExpected;
	protected boolean finished;
	protected ArrayList<LinkedList<ChunkVersion>> chunksByIndex = new ArrayList<LinkedList<ChunkVersion>>();
	protected ArrayList<LinkedList<PeerChunkInfo>> peersByIndex = new ArrayList<LinkedList<PeerChunkInfo>>();
	protected PeerSwarm swarm;
	protected final Logger logger = LoggerFactory.getLogger(ChunkAccumulator.class); 
	
	public ChunkAccumulator(PeerSwarm swarm, byte[] tag, int numChunksExpected) {
		this.swarm = swarm;
		this.tag = tag;
		this.numChunksExpected = numChunksExpected;
		
		for(int i = 0; i < numChunksExpected; i++) {
			chunksByIndex.add(new LinkedList<ChunkVersion>());
			peersByIndex.add(new LinkedList<PeerChunkInfo>());
		}
	}
	
	public synchronized boolean addChunk(int index, byte[] chunk, PeerConnection peer) throws IOException {
		if(finished) return true;
		boolean needsInsert = true;
		if(index >= numChunksExpected || index < 0) throw new EINVALException(Util.bytesToHex(tag) + ":" + index);
		ChunkVersion version = new ChunkVersion(peer.socket.swarm.config.getAccessor().getMaster().scratchStorage(), chunk);
		
		for(ChunkVersion existing : chunksByIndex.get(index)) {
			if(Arrays.equals(existing.hash, version.hash)) {
				needsInsert = false;
				version = existing;
				break;
			}
		}
		
		if(needsInsert) {
			chunksByIndex.get(index).add(version);
		}
		
		for(PeerChunkInfo peerInfo : peersByIndex.get(index)) {
			if(peerInfo.peer == peer) {
				logger.warn("Peer " + peerInfo.peer.socket.getAddress() + " sent multiple versions of chunk {} of tag {}; blacklisting.", index, Util.bytesToHex(tag));
				peer.blacklist();
			}
		}
		
		peersByIndex.get(index).add(new PeerChunkInfo(version, peer));
		return hasCandidatesForAllChunks() && trySolutions(new ArrayList<ChunkVersion>());
	}
	
	public boolean isFinished() {
		return finished;
	}
	
	protected boolean hasCandidatesForAllChunks() {
		for(int i = 0; i < numChunksExpected; i++) {
			if(chunksByIndex.get(i).size() == 0) return false;
		}
		
		System.out.println("All chunks acquired");
		return true;
	}
	
	protected boolean trySolutions(ArrayList<ChunkVersion> chunks) throws IOException {
		if(chunks.size() >= numChunksExpected) {
			return validate(chunks);
		}
		
		for(ChunkVersion version : chunksByIndex.get(chunks.size())) {
			chunks.add(version);
			if(trySolutions(chunks)) return true;
			chunks.remove(version);
		}
		
		return false;
	}
	
	protected boolean validate(ArrayList<ChunkVersion> chunks) throws IOException {
		byte[] allegedPage = makeFileBuffer(chunks);
		if(!swarm.config.validatePage(tag, allegedPage)) {
			return false;
		}
		
		finished = true;
		swarm.config.getStorage().write(Page.pathForTag(tag), allegedPage);
		burnHeretics(chunks);
		closeFiles();
		swarm.receivedPage(tag);
		return true;
	}
	
	protected byte[] makeFileBuffer(ArrayList<ChunkVersion> chunks) throws IOException {
		ByteBuffer assembled = ByteBuffer.allocate((chunks.size()-1) * chunks.get(0).getBytes().length + chunks.get(chunks.size()-1).getBytes().length);
		for(ChunkVersion chunk : chunks) {
			assembled.put(chunk.getBytes());
		}
		return assembled.array();
	}
	
	protected void burnHeretics(ArrayList<ChunkVersion> chunks) {
		for(int i = 0; i < numChunksExpected; i++) {
			ChunkVersion correctChunk = chunks.get(i); 
			for(PeerChunkInfo peerInfo : peersByIndex.get(i)) {
				if(peerInfo.version == correctChunk) continue;
				logger.warn("Peer " + peerInfo.peer.socket.getAddress() + " send invalid chunk; blacklisting.");
				peerInfo.peer.blacklist(); 
			}
		}
	}
	
	protected void closeFiles() throws IOException {
		for(LinkedList<ChunkVersion> list : chunksByIndex) {
			for(ChunkVersion version : list) {
				version.finish();
			}
		}
	}
}
