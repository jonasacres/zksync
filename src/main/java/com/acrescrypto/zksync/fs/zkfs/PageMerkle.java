package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import com.acrescrypto.zksync.crypto.Key;
import com.acrescrypto.zksync.crypto.SecureFile;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.utility.Util;

/** Merkle tree of page tags for a file. Each page tag is a signed hash of the page contents, and is needed to locate
 * a page in storage. */
public class PageMerkle {
	ZKArchive archive; /** archive to which this merkle belongs */
	RefTag tag; /** current reftag for tree content */
	PageMerkleNode[] nodes; /** all nodes in tree, indexed left-right top-bottom */
	int maxPages; /** page capacity for current tree size */
	int numPagesUsed; /** number of pages issued */
	
	public static byte[] tagForChunk(RefTag refTag, int chunk) {
		ByteBuffer chunkTagSource = ByteBuffer.allocate(refTag.hash.length+4);
		chunkTagSource.put(refTag.hash);
		chunkTagSource.putInt(chunk);
		
		Key authKey = refTag.config.deriveKey(ArchiveAccessor.KEY_INDEX_ARCHIVE, ArchiveAccessor.KEY_TYPE_AUTH, ArchiveAccessor.KEY_INDEX_PAGE_MERKLE, refTag.getHash());
		return authKey.authenticate(chunkTagSource.array());
	}
	
	/** path to a given chunk of the merkle tree in the underlying filesystem */
	public static String pathForChunk(RefTag refTag, int chunk) {
		return Page.pathForTag(tagForChunk(refTag, chunk));
	}
	
	/** initialize a PageMerkle from a file contents RefTag */
	public PageMerkle(RefTag tag) throws IOException {
		this.archive = tag.getArchive();
		this.tag = tag;
		
		switch(tag.getRefType()) {
		case RefTag.REF_TYPE_IMMEDIATE:
		case RefTag.REF_TYPE_INDIRECT:
			this.maxPages = this.numPagesUsed = 1;
			resize(1);
			this.nodes[0].tag = tag.getHash();
			break;
		case RefTag.REF_TYPE_2INDIRECT:
			read();
			break;
		}
	}
	
	public void assertExists() throws ENOENTException, IOException {
		if(!exists()) {
			throw new ENOENTException("pagemerkle for " + Util.bytesToHex(tag.getBytes()));
		}
	}
	
	public boolean exists() throws IOException {
		switch(tag.getRefType()) {
		case RefTag.REF_TYPE_IMMEDIATE: return true;
		case RefTag.REF_TYPE_INDIRECT:
			return archive.storage.exists(Page.pathForTag(nodes[0].tag));
		case RefTag.REF_TYPE_2INDIRECT:
			if(numChunks() <= 0) return false;
			for(int i = 0; i < numChunks(); i++) {
				if(!archive.storage.exists(Page.pathForTag(tagForChunk(tag, i)))) return false;
			}
			
			return true;
		default:
			return false;
		}
	}
	
	/** RefTag for file contents */
	public RefTag getRefTag() {
		// empty tree => 0 pages => contents = "", guaranteed to be an immediate
		if(nodes == null || nodes.length == 0) return RefTag.blank(archive);
		nodes[0].recalculate();
		
		int type;
		if(maxPages == 1) {
			if(nodes[0].tag.length < archive.crypto.hashLength()) {
				type = RefTag.REF_TYPE_IMMEDIATE;
			} else {
				type = RefTag.REF_TYPE_INDIRECT;
			}
		} else {
			type = RefTag.REF_TYPE_2INDIRECT;
		}
		
		return new RefTag(archive, nodes[0].tag, type, numPagesUsed);
	}
	
	/** set the tag for a given page number, resizing the tree as needed */
	public void setPageTag(int pageNum, byte[] pageTag) {
		if(pageNum >= maxPages) resize(pageNum+1);
		nodes[maxPages - 1 + pageNum].setTag(pageTag);
		numPagesUsed = Math.max(pageNum+1, numPagesUsed);
		if(tag.getRefType() != RefTag.REF_TYPE_IMMEDIATE) {
			archive.addPageTag(pageTag);
		}
	}
	
	/** get the tag for a given page number
	 * 
	 * @param pageNum
	 * @return
	 */
	public byte[] getPageTag(int pageNum) {
		return nodes[maxPages - 1 + pageNum].tag.clone();
	}
	
	/** size of plaintext serialization of this PageMerkle */
	public long plaintextSize() {
		return archive.crypto.hashLength()*(2*maxPages-1);
	}
	
	public int numChunks() {
		if(maxPages <= 1) return 0;
		return (int) Math.ceil( (double) plaintextSize() / archive.config.pageSize );
	}
	
	public int numPages() {
		return numPagesUsed;
	}
	
	/** write to storage */
	public RefTag commit() throws IOException {
		tag = getRefTag();
		if(tag.getRefType() != RefTag.REF_TYPE_2INDIRECT) return tag;
		
		int chunkCount = numChunks();
		
		ByteBuffer plaintext = ByteBuffer.allocate(nodes.length*archive.crypto.hashLength());
		
		for(PageMerkleNode node : nodes) plaintext.put(node.tag);
		for(int i = 0; i < chunkCount; i++) {
			int offset = i*archive.config.pageSize;
			int len = Math.min(archive.config.pageSize, plaintext.capacity() - offset);
			archive.addPageTag(tag.getHash());
			
			SecureFile
			  .atPath(archive.storage, pathForChunk(tag, i), cipherKey(tag), tag.getBytes(), (""+i).getBytes())
			  .write(plaintext.array(), offset, len, archive.config.getSerializedPageSize());
		}
		
		return tag;
	}
	
	/** read contents from storage */
	private void read() throws IOException {
		long expectedPages = tag.getNumPages();
		expectedPages = (int) Math.pow(2, Math.ceil(Math.log(expectedPages)/Math.log(2)));
		
		long expectedNodes = 2*expectedPages - 1;
		int expectedChunks = (int) Math.ceil((double) expectedNodes*archive.crypto.hashLength()/archive.config.pageSize);
		
		ByteBuffer readBuf = ByteBuffer.allocate((int) (expectedChunks*archive.config.pageSize));
		
		resize(expectedPages);
		
		/* TODO: Allow partial reads of PageMerkle */
		
		maxPages = (int) expectedPages;
		numPagesUsed = (int) tag.getNumPages();
		
		if(tag.getRefType() != RefTag.REF_TYPE_2INDIRECT) {
			nodes[0].tag = tag.getBytes();
			return;
		}
		
		for(int i = 0; i < expectedChunks; i++) {
			readBuf.put(SecureFile
					.atPath(archive.storage, pathForChunk(tag, i), cipherKey(tag), tag.getBytes(), (""+i).getBytes())
					.read());
		}
		
		readBuf.rewind();
		for(int i = 0; i < expectedNodes; i++) {
			byte[] tag = new byte[archive.crypto.hashLength()];
			readBuf.get(tag);
			nodes[i].tag = tag;
		}
		
		checkTreeIntegrity();
	}
	
	/** test that tree root tag is correct
	 * 
	 * @throws InvalidArchiveException root tag was not correct
	 */
	private void checkTreeIntegrity() {
		if(nodes.length == 0) return;
		byte[] treeRoot = nodes[0].tag.clone();
		for(int i = 0; i < nodes.length-1; i++) {
			nodes[i].markDirty();
		}
		
		nodes[0].recalculate();
		if(!Arrays.equals(nodes[0].tag, treeRoot)) {
			throw new InvalidArchiveException("Inconsistent merkle tree");
		}
	}

	/** resize the merkle tree to accommodate a new minimum number of nodes. may be bigger or smaller than previous
	 * minimum. */
	public void resize(long newMinNodes) {
		double log2 = Math.log(2);
		int newSize = newMinNodes > 0 ? (int) Math.pow(2, Math.ceil(Math.log(newMinNodes)/log2)) : 0;
		PageMerkleNode[] newNodes = new PageMerkleNode[Math.max(2*newSize-1, 1)];
		
		int numExistingNodes = nodes == null ? 0 : nodes.length;
		int d = (int) Math.round((Math.log(newNodes.length+1) - Math.log(numExistingNodes+1))/log2);
		
		/* Resizing the merkle tree is a matter of considering the subtree whose root node is:
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
			newNodes = nodes;
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
				tierThreshold = Math.min(tierThreshold, (1 << tier) + numExistingNodes - 1);
				
				if(minN <= n && n < tierThreshold) {
					// are we an existing node? must be deep enough (minN <= n), and left enough (n < tierThreshold) to be in our subtree.
					int m = (int) ((1 << tier) * dMult + n); // 2^(tier) * (2^-d - 1) + n
					newNodes[n] = nodes[m];
				} else {
					// new node: allocate a blank node
					newNodes[n] = new PageMerkleNode(archive.crypto);
					if(2*n+2 < newNodes.length) { // non-leaf node; setup parent/child references
						newNodes[n].left = newNodes[2*n+1];
						newNodes[n].right = newNodes[2*n+2];
						newNodes[n].left.parent = newNodes[n];
						newNodes[n].right.parent = newNodes[n];
					}
				}
			}
		}

		this.numPagesUsed = Math.min(numPagesUsed, newSize);
		this.maxPages = newSize;
		this.nodes = newNodes;
	}
	
	/** key used to encrypt serialized merkle tree. */
	private Key cipherKey(RefTag refTag) {
		return archive.config.deriveKey(ArchiveAccessor.KEY_INDEX_ARCHIVE, ArchiveAccessor.KEY_TYPE_CIPHER, ArchiveAccessor.KEY_INDEX_PAGE_MERKLE, refTag.getBytes());
	}
	
	/** test if a given page has a tag set yet or not. */
	public boolean hasTag(int pageNum) {
		if(pageNum >= numPagesUsed) return false;
		try {
			return !nodes[maxPages - 1 + pageNum].isBlank();
		} catch(ArrayIndexOutOfBoundsException exc) {
			return false;
		}
	}

	public byte[] tagForChunk(int index) {
		return PageMerkle.tagForChunk(tag, index);
	}
	
	public ZKArchive getArchive() {
		return archive;
	}
}
