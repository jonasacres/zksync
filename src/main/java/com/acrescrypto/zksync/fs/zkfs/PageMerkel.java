package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.Util;
import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.exceptions.NonexistentPageException;

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
	
	public byte[] getPageTag(int pageNum) throws NonexistentPageException {
		if(pageNum < 0 || pageNum >= numPages) {
			throw new NonexistentPageException(inode.getStat().getInodeId(), pageNum);
		}
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
			byte[] chunkCiphertext = cipherKey(getMerkelTag()).wrappedEncrypt(chunkText.array(),
					(int) fs.getPrivConfig().getPageSize());
			
			chunkTagSource.position(refTag.length);
			chunkTagSource.putInt(i);
			byte[] chunkTag = authKey(getMerkelTag()).authenticate(chunkTagSource.array());
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
		expectedPages = (int) Math.pow(2, Math.ceil(Math.log(expectedPages)/Math.log(2)));
		
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
				byte[] chunkTag = authKey(inode.getRefTag()).authenticate(chunkTagSource.array());
				String path = ZKFS.DATA_DIR + fs.pathForHash(chunkTag);
				byte[] chunkCiphertext;
				try {
					chunkCiphertext = fs.getStorage().read(path);
				} catch (IOException e) {
					throw new InaccessibleStorageException();
				}
				byte[] chunkPlaintext = cipherKey(inode.getRefTag()).wrappedDecrypt(chunkCiphertext);
				readBuf.put(chunkPlaintext);
			}
			
			readBuf.rewind();
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
		for(int i = 0; i < nodes.length-1; i++) {
			nodes[i].markDirty();
		}
		
		nodes[0].recalculate();
		if(!Arrays.equals(nodes[0].tag, treeRoot)) {
			throw new InvalidArchiveException("Inconsistent merkel tree");
		}
	}

	public void resize(int newMinNodes) {
		double log2 = Math.log(2);
		int newSize =  (int) Math.pow(2, Math.ceil(Math.log(newMinNodes)/log2));
		PageMerkelNode[] newNodes = new PageMerkelNode[Math.max(2*newSize-1, 1)];
		
		int numExistingNodes = nodes == null ? 0 : nodes.length;
		int d = (int) Math.round((Math.log(newNodes.length+1) - Math.log(numExistingNodes+1))/log2);
		
		/* Resizing the merkel tree is a matter of considering the subtree whose root node is:
		 *   1) left-most in its tier (i.e. how many levels down it is in the tree, starting from tier=0)
		 *   2) at tier |d| (the number of tiers we are adding or removing)
		 * If d == 0, then we're making no changes at all and we can just go home. We're done.
		 *    d  < 0, we're shrinking the tree, so we can just make the subtree be the new root. None of the tags
		 *            change for leaf or ancestor nodes, so all we have to do is relocate the nodes.
		 *    d  > 0, we're growing the tree, so we have to take our current root, and make it be the leftmost
		 *            subtree at tier d of the newer, bigger tree. All the new leaf nodes should have a default
		 *            value to reflect that they haven't been assigned tags yet, and all the new non-leaf nodes
		 *            need to be recalculated.
		 */
		
		if(d == 0) {
			return;
		} else if(d < 0) { // tree gets smaller
			for(int n = 0; n < newNodes.length; n++) {
				// every tier has a fixed offset in array indexes between old tree and new tree
				int tier = (int)(Math.log(n+1)/log2); // floor(log2(n+1))
				int diff = (1 << tier)*(1-(1 << (-d))); // 2^tier * (1-(2^(-d)))
				newNodes[n] = nodes[n-diff];
			}
		} else if(d > 0) { // tree gets bigger
			int minN = (1 << d) - 1; // 2^d - 1
			double dMult = 1.0/(1 << d) - 1.0; // 2^-d - 1
			
			for(int n = newNodes.length-1; n >= 0; n--) {
				int tier = (int) (Math.log(n+1)/log2); // floor(log2(n+1))
				int tierThreshold = 3*(1 << (tier - 1)) - 1; // 3*2^(tier-1). Anything above this is a new node.
				
				if(minN <= n && n < tierThreshold) {
					// are we an existing node? must be deep enough (minN <= n), and left enough (n < tierThreshold) to be in our subtree.
					int m = (int) ((1 << tier) * dMult + n); // 2^(tier) * (2^-d - 1) + n
					newNodes[n] = nodes[m];
				} else {
					// new node: allocate a blank node
					newNodes[n] = new PageMerkelNode(fs.crypto);
					if(2*n+2 < newNodes.length) { // non-leaf node; setup parent/child references
						newNodes[n].left = newNodes[2*n+1];
						newNodes[n].right = newNodes[2*n+2];
						newNodes[n].left.parent = newNodes[n];
						newNodes[n].right.parent = newNodes[n];
					}
				}
			}
		}
		this.numPages = newSize;
		this.nodes = newNodes;
	}
	
	private Key cipherKey(byte[] refTag) {
		return fs.deriveKey(ZKFS.KEY_TYPE_CIPHER, ZKFS.KEY_INDEX_PAGE_MERKEL, refTag);
	}
	
	private Key authKey(byte[] refTag) {
		return fs.deriveKey(ZKFS.KEY_TYPE_AUTH, ZKFS.KEY_INDEX_PAGE_MERKEL, refTag);
	}
	
	public void dumpToConsole(String caption) {
		int nodeCount = nodes == null ? 0 : nodes.length;
		System.out.println("Tree dump: " + caption);
		System.out.println("Inode: " + inode.getStat().getInodeId());
		System.out.println("Size (total nodes): " + nodeCount);
		System.out.println("Size (pages): " + numPages);
		for(int i = 0; i < nodeCount; i++) {
			System.out.printf("  Node %2d (%s): %s\n", i, nodes[i].dirty ? "dirty" : "clean", Util.bytesToHex(nodes[i].tag));
		}
		System.out.println("");
	}
}
