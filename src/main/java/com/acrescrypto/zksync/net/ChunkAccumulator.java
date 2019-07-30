package com.acrescrypto.zksync.net;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;
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
			this.hash = hashForChunk(chunk);
			write(chunk);
		}
		
		public byte[] getBytes() throws IOException {
			return scratchFS.read(path());
		}
		
		public void finish() throws IOException {
			scratchFS.unlink(path());
		}
		
		public boolean matches(byte[] chunk) {
			return Arrays.equals(hashForChunk(chunk), hash);
		}

		protected String path() {
			return Util.bytesToHex(hash);
		}
		
		protected byte[] hashForChunk(byte[] chunk) {
			CryptoSupport crypto = swarm.config.getAccessor().getMaster().getCrypto();
			return crypto.authenticate(tag.getTagBytes(), chunk);
		}
		
		protected void write(byte[] chunk) throws IOException {
			scratchFS.write(path(), chunk);
		}
	}
	
	protected StorageTag tag;
	protected int numChunksExpected;
	protected boolean finished;
	protected ArrayList<LinkedList<ChunkVersion>> chunksByIndex = new ArrayList<LinkedList<ChunkVersion>>();
	protected ArrayList<LinkedList<PeerChunkInfo>> peersByIndex = new ArrayList<LinkedList<PeerChunkInfo>>();
	protected PeerSwarm swarm;
	protected final Logger logger = LoggerFactory.getLogger(ChunkAccumulator.class); 
	
	public ChunkAccumulator(PeerSwarm swarm, StorageTag tag, int numChunksExpected) {
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
		if(index >= numChunksExpected || index < 0) throw new EINVALException(tag + ":" + index);
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
		
		boolean add = true;
		for(PeerChunkInfo peerInfo : peersByIndex.get(index)) {
			if(peerInfo.peer == peer) {
				// TODO API: (coverage) branch
				if(peerInfo.version.matches(chunk)) {
					add = false;
					break;
				}
				
				logger.warn("Peer " + peerInfo.peer.socket.getAddress() + " sent multiple versions of chunk {} of tag {}; blacklisting.", index, tag);
				peer.blacklist();
			}
		}
		
		if(add) {
			peersByIndex.get(index).add(new PeerChunkInfo(version, peer));
		}
		
		return hasCandidatesForAllChunks() && trySolutions(new ArrayList<ChunkVersion>());
	}
	
	public boolean isFinished() {
		return finished;
	}
	
	protected boolean hasCandidatesForAllChunks() {
		for(int i = 0; i < numChunksExpected; i++) {
			if(chunksByIndex.get(i).size() == 0) return false;
		}
		
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
		logger.debug("Swarm {} -: Storing validated page {}",
				Util.formatArchiveId(swarm.getConfig().getArchiveId()),
				tag);
		swarm.config.getStorage().write(tag.path(), allegedPage);
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
