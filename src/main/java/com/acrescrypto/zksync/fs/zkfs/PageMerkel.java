package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.exceptions.InaccessibleStorageException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.exceptions.NonexistentPageException;

public class PageMerkel {
	ZKArchive archive;
	RefTag tag;
	PageMerkelNode[] nodes;
	int numPages;
	
	public static String pathForChunk(RefTag refTag, int chunk) {
		ByteBuffer chunkTagSource = ByteBuffer.allocate(refTag.hash.length+4);
		chunkTagSource.put(refTag.hash);
		chunkTagSource.putInt(chunk);
		
		Key authKey = refTag.archive.deriveKey(ZKArchive.KEY_TYPE_AUTH, ZKArchive.KEY_INDEX_PAGE_MERKEL, refTag.getHash());
		byte[] chunkTag = authKey.authenticate(chunkTagSource.array());
		return ZKFS.pathForHash(chunkTag);
	}
	
	PageMerkel(RefTag tag) throws InaccessibleStorageException {
		this.archive = tag.archive;
		this.tag = tag;
		
		switch(tag.getRefType()) {
		case RefTag.REF_TYPE_IMMEDIATE:
		case RefTag.REF_TYPE_INDIRECT:
			setPageTag(0, tag.getHash());
			break;
		case RefTag.REF_TYPE_2INDIRECT:
			read();
			break;
		}
	}
	
	public RefTag getRefTag() {
		// empty tree => 0 pages => contents = "", guaranteed to be an immediate
		if(nodes == null || nodes.length == 0) return RefTag.blank(archive);
		nodes[0].recalculate();
		
		int type;
		if(numPages == 1) {
			if(nodes[0].tag.length < archive.crypto.hashLength()) {
				type = RefTag.REF_TYPE_IMMEDIATE;
			} else {
				type = RefTag.REF_TYPE_INDIRECT;
			}
		} else {
			type = RefTag.REF_TYPE_2INDIRECT;
		}
		return new RefTag(archive, nodes[0].tag, type, numPages);
	}
	
	public void setPageTag(int pageNum, byte[] pageTag) {
		if(pageNum >= numPages) resize(pageNum+1);
		nodes[numPages - 1 + pageNum].setTag(pageTag);
	}
	
	public byte[] getPageTag(int pageNum) throws NonexistentPageException {
		if(pageNum < 0 || pageNum >= numPages) {
			throw new NonexistentPageException(tag, pageNum);
		}
		return nodes[numPages - 1 + pageNum].tag.clone();
	}
	
	public long plaintextSize() {
		return archive.crypto.hashLength()*(2*numPages-1);
	}
	
	public RefTag commit() throws InaccessibleStorageException {
		tag = getRefTag();
		if(tag.getRefType() != RefTag.REF_TYPE_2INDIRECT) return tag;
		
		int chunkCount = (int) Math.ceil( (double) plaintextSize() / archive.privConfig.getPageSize() );
		
		ByteBuffer plaintext = ByteBuffer.allocate(nodes.length*archive.crypto.hashLength());
				
		for(PageMerkelNode node : nodes) plaintext.put(node.tag);
		for(int i = 0; i < chunkCount; i++) {
			ByteBuffer chunkText = ByteBuffer.wrap(plaintext.array(),
					(int) (i*archive.privConfig.getPageSize()),
					Math.min(archive.privConfig.getPageSize(), plaintext.capacity()));
			byte[] chunkCiphertext = cipherKey(getRefTag()).wrappedEncrypt(chunkText.array(),
					(int) archive.privConfig.getPageSize());
			
			String path = pathForChunk(tag, i);
			
			try {
				archive.storage.write(path, chunkCiphertext);
				archive.storage.squash(path);
			} catch (IOException e) {
				throw new InaccessibleStorageException();
			}
		}
		
		return tag;
	}
	
	private void read() throws InaccessibleStorageException {
		long expectedPages = tag.getNumChunks();
		expectedPages = (int) Math.pow(2, Math.ceil(Math.log(expectedPages)/Math.log(2)));
		
		long expectedNodes = 2*expectedPages - 1;
		int expectedChunks = (int) Math.ceil((double) expectedNodes*archive.crypto.hashLength()/archive.privConfig.getPageSize());
		
		ByteBuffer readBuf = ByteBuffer.allocate((int) (expectedChunks*archive.privConfig.getPageSize()));
		
		resize(expectedPages);
		
		// TODO: consider not requiring a full readBuf; can we rely on guarantee hashes won't cross chunk boundaries?
		
		if(tag.getRefType() == RefTag.REF_TYPE_2INDIRECT) {
			for(int i = 0; i < expectedChunks; i++) {
				String path = pathForChunk(tag, i);
				byte[] chunkCiphertext;
				try {
					chunkCiphertext = archive.storage.read(path);
				} catch (IOException e) {
					throw new InaccessibleStorageException();
				}
				byte[] chunkPlaintext = cipherKey(tag).wrappedDecrypt(chunkCiphertext);
				readBuf.put(chunkPlaintext);
			}
			
			readBuf.rewind();
			for(int i = 0; i < expectedNodes; i++) {
				byte[] tag = new byte[archive.crypto.hashLength()];
				readBuf.get(tag);
				nodes[i].tag = tag;
			}
			
			checkTreeIntegrity();
		} else {
			nodes[0].tag = tag.getBytes();
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

	public void resize(long newMinNodes) {
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
			this.numPages = newSize;
			return;
		} else if(d < 0) { // tree gets smaller
			for(int n = 0; n < newNodes.length; n++) {
				// every tier has a fixed offset in array indexes between old tree and new tree
				int tier = (int)(Math.log(n+1)/log2); // floor(log2(n+1))
				int diff = (1 << tier)*(1-(1 << (-d))); // 2^tier * (1-(2^(-d)))
				newNodes[n] = nodes[n-diff];
			}
		} else { // tree gets bigger
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
					newNodes[n] = new PageMerkelNode(archive.crypto);
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
	
	private Key cipherKey(RefTag refTag) {
		return archive.deriveKey(ZKArchive.KEY_TYPE_CIPHER, ZKArchive.KEY_INDEX_PAGE_MERKEL, refTag.getBytes());
	}
}
