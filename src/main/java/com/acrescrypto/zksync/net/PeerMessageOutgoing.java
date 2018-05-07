package com.acrescrypto.zksync.net;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.SocketClosedException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.zkfs.RefTag;

public class PeerMessageOutgoing extends PeerMessage {
	protected ByteBuffer txBuf;
	protected boolean txEOF;
	protected InputStream txPayload;
	protected File file;
	protected RefTag refTag;
	protected Queue<Integer> chunkList = new LinkedList<Integer>();
	private Logger logger = LoggerFactory.getLogger(PeerMessageOutgoing.class);
	
	public PeerMessageOutgoing(PeerConnection connection, byte cmd, RefTag refTag, File file) throws IOException {
		this.connection = connection;
		this.cmd = cmd;
		this.txPayload = new ByteArrayInputStream(refTag.getBytes());
		this.file = file;
		this.msgId = connection.socket.issueMessageId();
		buildChunkList();
		runTxThread();
	}

	public PeerMessageOutgoing(PeerConnection connection, byte cmd, InputStream txPayload) {
		this.connection = connection;
		this.cmd = cmd;
		this.txPayload = txPayload;
		this.msgId = connection.socket.issueMessageId();
		runTxThread();
	}
	
	public PeerMessageOutgoing(PeerConnection connection, byte cmd, byte[] txPayload) {
		this(connection, cmd, new ByteArrayInputStream(txPayload));
	}

	public boolean txClosed() {
		return txEOF;
	}

	protected void runTxThread() {
		new Thread(() -> {
			while(!txEOF) {
				waitForTxBufAvailability();
				loadTxBuf();
				if(txEOF || !txBuf.hasRemaining()) {
					addTxHeader();
					if(connection.isPausable(cmd)) {
						try {
							connection.waitForUnpause();
						} catch (SocketClosedException exc) {
							logger.debug("Socket closed while sending message type {} (id={})", cmd, msgId, exc);
							break;
						}
					}
					connection.socket.dataReady(this);
				}
			}
		}).start();
	}
	
	protected void waitForTxBufAvailability() {
		while(!txEOF && !txBuf.hasRemaining()) {
			try {
				this.wait();
			} catch (InterruptedException exc) {}
		}
	}
	
	protected void loadTxBuf() {
		try {
			while(txBuf.remaining() > 0 && !txEOF) {
				if(txPayload != null) loadFromTxPayload();
				else if(file != null) loadFromFile();
				else txEOF = true;
			}
		} catch (IOException exc) {
			connection.socket.ioexception(exc);
		}
	}
	
	protected void loadFromTxPayload() throws IOException {
		int r = txPayload.read(txBuf.array(), txBuf.position(), txBuf.remaining());
		if(r == -1) {
			txPayload = null;
		} else {
			txBuf.position(txBuf.position() + r);
		}
	}
	
	protected void loadFromFile() throws IOException {
		if(chunkList.isEmpty() || !connection.wantsFile(refTag)) {
			file.close();
			file = null;
			return;
		}
		
		int chunk = chunkList.remove();
		if(!connection.wantsChunk(refTag, chunk)) return; // outer loop will cycle through the chunk queue
		int readBytes = Math.min(txBuf.remaining(), FILE_CHUNK_SIZE);
		file.seek(chunk*FILE_CHUNK_SIZE, File.SEEK_SET);
		txBuf.put(file.read(readBytes));
	}
	
	protected void addTxHeader() {
		ByteBuffer headerBuf = ByteBuffer.wrap(txBuf.array());
		headerBuf.putInt(msgId);
		headerBuf.putInt(txBuf.position()-HEADER_LENGTH);
		headerBuf.put(cmd);
		headerBuf.put((byte) (flags | (txEOF ? FLAG_FINAL : 0x00)));
		headerBuf.putShort((short) 0);
	}
	
	protected synchronized void clearTxBuf() {
		txBuf.position(HEADER_LENGTH);
		this.notifyAll();
	}
	
	protected void buildChunkList() throws IOException {
		if(file == null) return;
		
		int numChunks = (int) Math.ceil((double) file.getStat().getSize()/FILE_CHUNK_SIZE);
		int[] chunkArray = new int[numChunks];
		for(int i = 0; i < numChunks; i++) {
			int j = (int) ((i+1)*Math.random());
			if(j != i) {
				chunkArray[i] = chunkArray[j];
			}
			chunkArray[j] = i;
		}
		
		for(int idx : chunkArray) {
			chunkList.add(idx);
		}
	}
}
