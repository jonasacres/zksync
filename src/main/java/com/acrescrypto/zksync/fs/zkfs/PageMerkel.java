package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;

public class PageMerkel {
	ZKFS fs;
	Inode inode;
	PageMerkelNode[] nodes;
	int numPages;
	
	PageMerkel(ZKFS fs, Inode inode) throws InaccessibleStorageException {
		this.fs = fs;
		this.inode = inode;
		if(inode.getRefTag() != null) {
			switch(inode.getRefType()) {
			case Inode.REF_TYPE_IMMEDIATE:
			case Inode.REF_TYPE_INDIRECT:
				setPageTag(0, inode.getRefTag());
				break;
			case Inode.REF_TYPE_2INDIRECT:
				read();
				break;
			}
		}
	}
	
	public byte[] getMerkelTag() {
		// empty tree => 0 pages => contents = "", guaranteed to be an immediate
		if(nodes == null || nodes.length == 0) return new byte[] {};
		nodes[0].recalculate();
		return nodes[0].tag.clone();
	}
	
	public void setPageTag(int pageNum, byte[] pageTag) {
		if(pageNum >= numPages) resize(pageNum+1);
		nodes[numPages - 1 + pageNum].setTag(pageTag);
	}
	
	public byte[] getPageTag(int pageNum) {
		return nodes[numPages - 1 + pageNum].tag.clone();
	}
	
	public long plaintextSize() {
		return fs.getCrypto().hashLength()*(2*numPages-1);
	}
	
	public byte[] commit() throws InaccessibleStorageException {
		int chunkCount = (int) Math.ceil( (double) plaintextSize() / fs.getPrivConfig().getPageSize() );
		
		ByteBuffer plaintext = ByteBuffer.allocate(nodes.length*fs.getCrypto().hashLength());
		nodes[0].recalculate();
		byte[] refTag = nodes[0].tag.clone();
		ByteBuffer chunkTagSource = ByteBuffer.allocate(refTag.length+4);
		chunkTagSource.put(refTag);
				
		for(PageMerkelNode node : nodes) plaintext.put(node.tag);
		for(int i = 0; i < chunkCount; i++) {
			ByteBuffer chunkText = ByteBuffer.wrap(plaintext.array(),
					(int) (i*fs.getPrivConfig().getPageSize()),
					Math.min(fs.getPrivConfig().getPageSize(), plaintext.capacity()));
			byte[] chunkCiphertext = cipherKey().wrappedEncrypt(chunkText.array(),
					(int) fs.getPrivConfig().getPageSize());
			
			chunkTagSource.position(refTag.length);
			chunkTagSource.putInt(i);
			byte[] chunkTag = authKey().authenticate(chunkTagSource.array());
			String path = ZKFS.DATA_DIR + fs.getStorage().pathForHash(chunkTag);
			System.out.printf("Writing path for merkel chunk index %d (%d total): %s\n", i, chunkCount, path);
			try {
				fs.getStorage().write(path, chunkCiphertext);
				fs.getStorage().squash(path);
			} catch (IOException e) {
				throw new InaccessibleStorageException();
			}
		}
		
		return refTag;
	}
	
	private void read() throws InaccessibleStorageException {
		int expectedPages = (int) Math.ceil((double) inode.getStat().getSize()/fs.getPrivConfig().getPageSize());
		int expectedNodes = 2*expectedPages - 1;
		int expectedChunks = (int) Math.ceil((double) expectedNodes*fs.getCrypto().hashLength()/fs.getPrivConfig().getPageSize());
		
		ByteBuffer readBuf = ByteBuffer.allocate((int) (expectedChunks*fs.getPrivConfig().getPageSize()));
		ByteBuffer chunkTagSource = ByteBuffer.allocate(inode.getRefTag().length+4);
		chunkTagSource.put(inode.getRefTag());
		
		resize(expectedPages);
		
		// TODO: consider not requiring a full readBuf; can we rely on guarantee hashes won't cross chunk boundaries?
		
		if(inode.getRefType() == Inode.REF_TYPE_2INDIRECT) {
			for(int i = 0; i < expectedChunks; i++) {
				chunkTagSource.position(inode.getRefTag().length);
				chunkTagSource.putInt(i);
				byte[] chunkTag = authKey().authenticate(chunkTagSource.array());
				String path = ZKFS.DATA_DIR + fs.pathForHash(chunkTag);
				System.out.printf("Reading merkel chunk %d (%d total): %s\n", i, expectedChunks, path);
				byte[] chunkCiphertext;
				try {
					chunkCiphertext = fs.getStorage().read(path);
				} catch (IOException e) {
					throw new InaccessibleStorageException();
				}
				byte[] chunkPlaintext = cipherKey().wrappedDecrypt(chunkCiphertext);
				readBuf.put(chunkPlaintext);
			}
		
			for(int i = 0; i < expectedNodes; i++) {
				byte[] tag = new byte[fs.getCrypto().hashLength()];
				readBuf.get(tag);
				nodes[i].tag = tag;
			}
			
			checkTreeIntegrity();
		} else {
			nodes[0].tag = inode.getRefTag().clone();
		}
	}
	
	private void checkTreeIntegrity() {
		if(nodes.length == 0) return;
		byte[] treeRoot = nodes[0].tag.clone();
		for(int i = 0; i < numPages-1; i++) {
			nodes[i].markDirty();
		}
		
		nodes[0].recalculate();
		if(!Arrays.equals(nodes[0].tag, treeRoot)) throw new InvalidArchiveException("Inconsistent merkel tree");
	}

	public void resize(int newMinNodes) {
		double to_log2 = 1.0/Math.log(2);
		int newSize =  (int) Math.pow(2, Math.ceil(Math.log(newMinNodes)*to_log2));
		PageMerkelNode[] newNodes = new PageMerkelNode[Math.max(2*newSize-1, 1)];
		
		int numExistingNodes = nodes == null ? 0 : nodes.length;
		int d = (int) Math.round((Math.log(newNodes.length+1) - Math.log(numExistingNodes+1))*to_log2);
		int minN = (1 << d) - 1;
		double dMult = 1.0/(1 << d) - 1.0;
		
		for(int n = newNodes.length-1; n >= 0; n--) {
			int tier = (int) (Math.log(n+1)*to_log2);
			int tierThreshold = 3*(1 << (tier - 1)) - 1;
			
			if(minN <= n && n < tierThreshold) {
				int m = (int) ((1 << tier) * dMult + n);
				newNodes[n] = nodes[m];
			} else {
				newNodes[n] = new PageMerkelNode(fs.crypto);
				if(2*n+2 < newNodes.length) {
					newNodes[n].left = newNodes[2*n+1];
					newNodes[n].right = newNodes[2*n+2];
					newNodes[n].left.parent = newNodes[n];
					newNodes[n].right.parent = newNodes[n];
				}
			}
		}
		
		newNodes[0].markDirty();
		newNodes[0].recalculate();
		this.numPages = newSize;
		this.nodes = newNodes;
	}
	
	private Key cipherKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_CIPHER, ZKFS.KEY_INDEX_PAGE_MERKEL, getMerkelTag());
	}
	
	private Key authKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_AUTH, ZKFS.KEY_INDEX_PAGE_MERKEL, getMerkelTag());
	}
}
