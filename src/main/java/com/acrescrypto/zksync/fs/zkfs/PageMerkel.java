package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;

public class PageMerkel {
	ZKFS fs;
	Inode inode;
	PageMerkelNode[] nodes;
	int numPages;
	
	PageMerkel(ZKFS fs, Inode inode) throws InaccessibleStorageException {
		this.fs = fs;
		this.inode = inode;
		read();
	}
	
	public byte[] getMerkelTag() {
		// merkel tags are only used if we have 2 or more pages, so the tag of an empty merkel tree can be undefined
		// this implementation just uses a hash-sized array of zeroes
		
		if(nodes == null || nodes.length == 0) return new byte[fs.getCrypto().hashLength()];
		nodes[0].recalculate();
		return nodes[0].tag.clone();
	}
	
	public void setPageTag(int pageNum, byte[] pageTag) {
		if(pageNum >= numPages) setupTree(pageNum);
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
					(int) fs.getPrivConfig().getPageSize());
			byte[] chunkCiphertext = cipherKey().wrappedEncrypt(chunkText.array(),
					(int) fs.getPrivConfig().getPageSize());
			
			chunkTagSource.position(refTag.length);
			chunkTagSource.putInt(i);
			byte[] chunkTag = authKey().authenticate(chunkTagSource.array());
			String path = ZKFS.DATA_DIR + fs.getStorage().pathForHash(chunkTag);
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
		
		setupTree(expectedPages);
		
		// TODO: consider not requiring a full readBuf; can we rely on guarantee hashes won't cross chunk boundaries?
		
		if(inode.getRefType() == Inode.REF_TYPE_2INDIRECT) {
			for(int i = 0; i < expectedChunks; i++) {
				chunkTagSource.position(inode.getRefTag().length);
				chunkTagSource.putInt(i);
				byte[] chunkTag = authKey().authenticate(chunkTagSource.array());
				String path = ZKFS.DATA_DIR + fs.pathForHash(chunkTag);
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
		} else {
			nodes[0].tag = inode.getRefTag().clone();
		}
	}

	private void setupTree(int newSize) {
		newSize = (int) Math.pow(2, Math.ceil(Math.log(newSize)/Math.log(2)));
		PageMerkelNode[] newNodes = new PageMerkelNode[2*newSize-1];
		for(int i = 0; i < newNodes.length; i++) {
			// allocate the new nodes
			newNodes[i] = new PageMerkelNode(fs.getCrypto());
		}
		
		for(int i = 0; i < newNodes.length; i++) {
			PageMerkelNode node = newNodes[i];
			// set up parent/child structure
			if(i > 0) node.parent = newNodes[(int) (Math.ceil(i/2)-1)];
			if(i < newSize-1) {
				node.left = newNodes[2*i+1];
				node.right = newNodes[2*i+2];
			} else if(this.nodes != null) {
				// copy existing hash values in case we are resizing
				int existingIndex = i - newSize - 1 + (nodes.length+1)/2;
				if(existingIndex < nodes.length) {
					newNodes[i].setTag(nodes[i].tag);
				}
			}
		}
		
		this.numPages = newSize;
		this.nodes = newNodes;
		nodes[0].recalculate();
	}
	
	private Key cipherKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_CIPHER, ZKFS.KEY_INDEX_PAGE_MERKEL, getMerkelTag());
	}
	
	private Key authKey() {
		return fs.deriveKey(ZKFS.KEY_TYPE_AUTH, ZKFS.KEY_INDEX_PAGE_MERKEL, getMerkelTag());
	}
}
