package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.StorageTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.PageQueue.ChunkQueueItem;
import com.acrescrypto.zksync.net.PageQueue.ChunkReference;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class PageQueueTest {
	static ZKArchive archive;
	static ZKMaster master;
	static Inode inodeImmediate, inodeIndirect, inode2Indirect;
	static RevisionTag revTag, secondRevTag;
	static LinkedList<Inode> inodesInRevision, inodesInSecondRevision;
	static StorageTag pageTag;
	static int numChunks;
	
	PageQueue queue;
	
	public static Inode addFile(ZKFS fs, LinkedList<Inode> list, String path, byte[] contents) throws IOException {
		fs.write(path, contents);
		Inode inode = fs.inodeForPath(path);
		if(list != null) list.add(inode);
		return inode;
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		numChunks = (int) Math.ceil((double) archive.getConfig().getPageSize()/PeerMessage.FILE_CHUNK_SIZE);
		
		inodesInRevision = new LinkedList<>();
		inodesInSecondRevision = new LinkedList<>();
		
		ZKFS fs = archive.openBlank();
		inodeImmediate = addFile(fs, inodesInRevision, "immediate", new byte[archive.getCrypto().hashLength()-1]);
		inodeIndirect = addFile(fs, inodesInRevision, "indirect", new byte[archive.getConfig().getPageSize()]);
		inode2Indirect = addFile(fs, inodesInRevision, "2indirect", new byte[10*archive.getConfig().getPageSize()]);
		fs.mkdir("dir1");
		fs.mkdir("dir2");
		fs.mkdir("dir2/subdir");
		revTag = fs.commit();
		
		Inode inode = addFile(fs, null, "sample", new byte[10*archive.getConfig().getPageSize()]);
		pageTag = new PageTree(inode).getPageTag(1);
		fs.commitAndClose();
		
		fs = archive.openRevision(revTag);		
		
		byte[] ones = new byte[2*archive.getConfig().getPageSize()];
		for(int i = 0; i < ones.length; i++) ones[i] = 1;
		
		addFile(fs, inodesInSecondRevision, "2indirect", ones);
		for(int i = 0; i < 64; i++) {
			addFile(fs, inodesInSecondRevision, "filler"+i, new byte[archive.getConfig().getPageSize()]);
		}
		secondRevTag = fs.commitAndClose();
	}
	
	@Before
	public void beforeEach() {
		queue = new PageQueue(archive.getConfig());
	}
	
	@After
	public void afterEach() {
		queue.close();
	}
	
	@AfterClass
	public static void afterAll() {
		archive.close();
		master.close();
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}
	
	public void checkMatchRange(int runs, int[] matches) {
		double p = Math.exp(-1);
		double r = runs;
		double f = 1;
		for(int i = 0; i < 4; i++) {
			if(i > 1) f /= i;
			assertTrue(Math.abs(matches[i]/r - p*f) < 0.07);
		}
	}
	
	public HashSet<Long> expectedPageTagsForInode(Inode inode) throws IOException {
		HashSet<Long> expectedTags = new HashSet<Long>();
		if(inode.getRefTag().getRefType() == RefTag.REF_TYPE_IMMEDIATE) return expectedTags;
		
		PageTree tree = new PageTree(inode);
		for(int i = 0; i < tree.numPages(); i++) {
			expectedTags.add(tree.getPageTag(i).shortTag());
		}
		
		if(inode.getRefTag().getRefType() == RefTag.REF_TYPE_2INDIRECT) {
			for(int i = 0; i < tree.numChunks(); i++) {
				expectedTags.add(tree.tagForChunk(i).shortTag());
			}
		}
		
		return expectedTags;
	}

	public HashSet<Long> expectedPageTagsForRevTag(RevisionTag revTag) throws IOException {
		HashSet<Long> expectedTags = new HashSet<Long>();
		try(ZKFS fs = revTag.readOnlyFS()) {
			expectedTags.add(fs.getInodeTable().getInode().getRefTag().getStorageTag().shortTag());
			for(int i = 0; i < fs.getInodeTable().nextInodeId(); i++) {
				expectedTags.addAll(expectedPageTagsForInode(fs.getInodeTable().inodeWithId(i)));
			}
		}
		
		return expectedTags;
	}
	
	public HashSet<Long> expectedStructuralPageTagsForRevTag(RevisionTag revTag) throws IOException {
		HashSet<Long> expectedTags = new HashSet<Long>();
		try(ZKFS fs = revTag.readOnlyFS()) {
			expectedTags.add(fs.getInodeTable().getInode().getRefTag().getStorageTag().shortTag());
			for(int i = 0; i < fs.getInodeTable().nextInodeId(); i++) {
				Inode inode = fs.getInodeTable().inodeWithId(i);
				if(!inode.getStat().isDirectory()) continue;
				expectedTags.addAll(expectedPageTagsForInode(inode));
			}
		}
		
		return expectedTags;
	}
	
	public void expectTagsFromQueue(HashSet<Long> expected) throws IOException {
		HashSet<Long> seenTags = new HashSet<Long>();
		HashSet<Integer> seenChunks = null;
		long lastTag = 0;
		
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			long shortTag = ref.tag.shortTag();
			assertTrue(expected.contains(shortTag));
			assertFalse(seenTags.contains(shortTag));
			
			if(lastTag != shortTag) {
				if(lastTag != 0) {
					assertEquals(numChunks, seenChunks.size());
					seenTags.add(lastTag);
				}
				
				lastTag = shortTag;
				seenChunks = new HashSet<Integer>();
			}
			
			assertFalse(seenChunks.contains(ref.index));
			seenChunks.add(ref.index);
		}
		
		assertEquals(numChunks, seenChunks.size());
		seenTags.add(lastTag);
		
		assertEquals(expected.size(), seenTags.size());
	}
	
	public void assertQueueDrainOfSize(int size) {
		int numChunksSeen = 0;
		
		while(queue.hasNextChunk()) {
			numChunksSeen++;
			queue.nextChunk();
		}
		
		assertEquals(size, numChunksSeen);
	}
	
	@Test
	public void testHasNextChunkReturnsFalseIfQueueEmpty() {
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testHasNextChunkReturnsFalseIfQueueHasOnlyGarbage() {
		queue.addPageTag(0, 0); // non-existent page, can't expand into any chunks
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testHasNextChunkReturnsTrueIfQueueHasChunkAtHead() throws IOException {
		queue.addPageTag(0, pageTag);
		queue.unpackNextReference();
		assertTrue(queue.itemsByPriority.peek() instanceof ChunkQueueItem);
		assertTrue(queue.hasNextChunk());
	}
	
	@Test
	public void testHasNextChunkReturnsTrueIfQueueHasNonchunkAtHead() throws IOException {
		queue.addPageTag(0, pageTag);
		assertFalse(queue.itemsByPriority.peek() instanceof ChunkQueueItem);
		assertTrue(queue.hasNextChunk());
	}
	
	@Test
	public void testNextChunkReturnsImmediatelyWhenAvailable() throws IOException {
		queue.addPageTag(0, pageTag);
		assertNotNull(queue.nextChunk());
	}
	
	@Test
	public void testNextChunkBlocksWhenNotAvailable() throws InterruptedException {
		class Holder { boolean passed; };
		Holder unblocked = new Holder(), queued = new Holder();
		
		Thread thread = new Thread(()-> {
			try {
				// wait long enough for the parent thread to block... (no science to this number, just has to be "big enough")
				Thread.sleep(10);
			} catch(InterruptedException exc) { fail(); }
			
			synchronized(queue) {
				assertFalse(unblocked.passed); // true if parent got past the block
				try {
					queue.addPageTag(0, pageTag);
				} catch (IOException e) {
					e.printStackTrace();
					fail();
				} // unblock the parent
				queued.passed = true;
			}
		});
		
		// queue is empty, so nextChunk() should block
		synchronized(queue) {
			thread.start();
			assertFalse(queued.passed);
			assertFalse(queue.hasNextChunk());
			assertNotNull(queue.nextChunk()); // waits on queue's monitor, hence we synchronize on that
			assertTrue(queued.passed);
			unblocked.passed = true;
		}
		thread.join();
	}
	
	@Test
	public void testNextChunkPrioritizesItems() {
		int size = 128, base = -64;
		Shuffler shuffler = new Shuffler(size);
		
		// insert priorities base, base+1, ..., size-1 in random order...
		while(shuffler.hasNext()) {
			int priority = shuffler.next() + base;
			ChunkReference ref = queue.new ChunkReference(archive.getStorage(),
					pageTag,
					priority);
			queue.addChunkReference(priority, ref);
		}
		
		// read them back in sorted order size-1, size-2, ..., base
		int priority = size + base;
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			assertEquals(priority-1, ref.index);
			priority = ref.index;
		}
		
		// we got to base, so we saw everything
		assertEquals(base, priority);
	}
	
	@Test
	public void testStartSendingEverythingSendsEverything() throws IOException {
		Collection<String> paths = archive.getStorage().opendir("/").listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES);
		HashSet<String> tags = new HashSet<String>();
		HashMap<String,Integer> seenCount = new HashMap<String,Integer>();
		
		for(String path : paths) {
			tags.add(path);
		}
		
		// send everything, note how many times we see each tag
		queue.startSendingEverything();		
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			String path = ref.tag.path(); // use paths since Strings fit nicely in the HashMap
			assertTrue(tags.contains(path)); // it's an error if we send something that's not a real tag
			seenCount.put(path, seenCount.getOrDefault(path, 0)+1);
		}
		
		// should have seen all the chunks of all the tags
		assertTrue(seenCount.size() == tags.size());
		for(int count : seenCount.values()) {
			assertEquals(numChunks, count);
		}
	}
	
	@Test @Ignore // ignored due to intermittent failures. needs better design.
	public void testEverythingSendsInShuffledOrder() throws IOException {
		int[] matchCounts = new int[1024];
		int n = 500;
		
		for(int i = 0; i < n; i++) {
			LinkedList<Long> seenTags = new LinkedList<Long>();
			
			// send everything, note the order we get tags in
			queue.startSendingEverything();
			while(queue.hasNextChunk()) {
				long shortTag = queue.nextChunk().tag.shortTag();
				if(seenTags.isEmpty() || seenTags.peekLast() != shortTag) {
					seenTags.add(shortTag);
				}
			}
			
			// go again, count up how many times the same tag comes at the same position in the lineup
			queue.startSendingEverything();
			int pagesSeen = 0, matches = 0;
			long lastTag = -1;
			while(queue.hasNextChunk()) {
				long shortTag = queue.nextChunk().tag.shortTag();
				if(lastTag == -1 || lastTag != shortTag) {
					lastTag = shortTag;
					if(seenTags.get(pagesSeen++) == shortTag) matches++;
				}
			}
			
			// should be mostly different
			matchCounts[matches]++;
		}
		
		/* Admittedly, I started with the base numbers expected from the distribution, and fine-tuned until the false positives stopped.
		 * It'd be worth designing a better test sometime, but for now, I'm convinced the shuffler works well enough for the purposes of ensuring
		 * peers don't flood each other with the exact same pages and chunks.
		 * */
		checkMatchRange(n, matchCounts);
	}
	
	@Test
	public void testStopSendingEverything() {
		// ask for everything, read a chunk, ask to stop
		queue.startSendingEverything();
		queue.nextChunk(); // let it break things down so we get chunk entries
		queue.stopSendingEverything();
		
		// everything, including the queued chunk entries, should be gone
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testStopSendingEverythingDoesntStopOtherRequests() throws IOException {
		// ask for everything, read a chunk, ask to stop
		queue.startSendingEverything();
		do {} while(pageTag.equals(queue.nextChunk().tag));
		queue.addPageTag(PageQueue.DEFAULT_EVERYTHING_PRIORITY-1, pageTag);
		queue.stopSendingEverything();
		
		// clear out the remainder of the current page
		for(int i = 0; i < numChunks-1; i++) {
			queue.nextChunk();
		}
		
		// but we have more data, and it's our requested page tag
		assertTrue(queue.hasNextChunk());
		assertTrue(pageTag.equals(queue.nextChunk().tag));
	}
	
	@Test
	public void testSendEverythingPriority() throws IOException {
		// ask for everything, and get a chunk so the queue has a bunch of chunks at the front at everything's priority
		// (also make sure that the chunk isn't for the page we're about to ask for)
		queue.startSendingEverything();
		do {} while(pageTag.equals(queue.nextChunk().tag));
		
		// ask for a page at a priority slightly higher than everything
		queue.addPageTag(PageQueue.DEFAULT_EVERYTHING_PRIORITY+1, pageTag);
		
		// the next chunk should be from that page
		assertTrue(pageTag.equals(queue.nextChunk().tag));
	}
	
	@Test
	public void testAddPageTagEnqueuesAllChunksInPage() throws IOException {
		queue.addPageTag(0, pageTag);
		HashSet<Integer> seenIndexes = new HashSet<Integer>();
		
		// now make sure every chunk we get has the requested tag and a sensible index that we've not seen before
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			assertTrue(pageTag.equals(ref.tag));
			assertTrue(0 <= ref.index);
			assertTrue(ref.index < numChunks);
			assertFalse(seenIndexes.contains(ref.index));
			seenIndexes.add(ref.index);
		}
		
		// we should have seen all the chunks
		assertEquals(numChunks, seenIndexes.size());
	}
	
	@Test
	public void testAddPageTagHonorsPrioritySettingBeforeChunking() throws IOException {
		assertFalse(pageTag.equals(inodeIndirect.getRefTag().getStorageTag())); // nothing up my sleeves...
		// add a low-priority tag, then a high-priority one
		queue.addPageTag(0, pageTag);
		queue.addPageTag(1, inodeIndirect.getRefTag().getStorageTag());
		
		// get the next chunk -- should be for the high-priority request
		assertTrue(queue.hasNextChunk());
		assertTrue(inodeIndirect.getRefTag().getStorageTag().equals(queue.nextChunk().tag));
		
		// wipe the slate and start over, this time with high-priority first
		queue.stopAll();
		queue.addPageTag(1, pageTag);
		queue.addPageTag(0, inodeIndirect.getRefTag().getStorageTag());
		assertTrue(queue.hasNextChunk());
		assertTrue(pageTag.equals(queue.nextChunk().tag));
	}
	
	@Test
	public void testAddPageTagHonorsPrioritySettingAfterChunking() throws IOException {
		assertFalse(pageTag.equals(inodeIndirect.getRefTag().getStorageTag()));
		// add a low-priority tag, then ask for a chunk to get its chunks queued up
		queue.addPageTag(0, pageTag);
		queue.nextChunk();
		
		// now add a high-priority request. the next chunk should be for that request.
		queue.addPageTag(1, inodeIndirect.getRefTag().getStorageTag());
		assertTrue(queue.hasNextChunk());
		assertTrue(inodeIndirect.getRefTag().getStorageTag().equals(queue.nextChunk().tag));
	}
	
	@Test
	public void testAddPageTagSendsChunksInShuffledOrder() throws IOException {
		int[] matchCounts = new int[numChunks+1];
		int numIterations = 10000;
		
		for(int i = 0; i < numIterations; i++) {
			// ask for a page, see what order the chunks come in
			Shuffler.purgeFixedOrderings();
			LinkedList<Integer> seenChunks = new LinkedList<Integer>();
			queue.addPageTag(0, pageTag);
			while(queue.hasNextChunk()) {
				seenChunks.add(queue.nextChunk().index); 
			}
			
			// force a reshuffle and try again, see how many places in the lineup match
			Shuffler.purgeFixedOrderings();
			queue.addPageTag(0, pageTag);
			int chunkNum = 0, matches = 0;
			while(queue.hasNextChunk()) {
				if(queue.nextChunk().index == seenChunks.get(chunkNum++)) {
					matches++;
				}
			}
			
			matchCounts[matches]++;
		}
		
		// make sure the observed results roughly follow the expected probability distribution
		checkMatchRange(numIterations, matchCounts);
	}
	
	@Test
	public void testAddPageTagToleratesNonexistentPages() throws IOException {
		// ask for a nonexistent page; queue should still be empty
		queue.addPageTag(0, 0);
		assertFalse(queue.hasNextChunk());
		
		// now for fun: ask for a real page, add the non-existent page, drain the queue. should still get the chunks.
		queue.addPageTag(0, pageTag);
		queue.addPageTag(0, 0);
		assertQueueDrainOfSize(numChunks);
		
		// and do that again, except the non-existent page goes first
		queue.addPageTag(0, 0);
		queue.addPageTag(0, pageTag);
		assertQueueDrainOfSize(numChunks);
	}
	
	@Test
	public void testAddPageTagAllowsReprioritization() throws IOException {
		// low-priority tag
		queue.addPageTag(0, pageTag);
		
		// now add a high-priority request and grab a chunk
		queue.addPageTag(1, inodeIndirect.getRefTag().getStorageTag());
		queue.nextChunk();
		
		// now reprioritize; our formerly low-priority tag should bump up to the head of the queue
		queue.addPageTag(2, pageTag);
		assertTrue(pageTag.equals(queue.nextChunk().tag));
	}
	
	@Test
	public void testAddPageTagAllowsCancellation() throws IOException {
		// low-priority tag
		queue.addPageTag(0, pageTag);
		queue.nextChunk();
		queue.addPageTag(Integer.MIN_VALUE, pageTag);
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testAddInodeContentsEnqueuesAllPagesInRefTag() throws IOException {
		HashSet<Long> seenPageTags = new HashSet<Long>();
		HashSet<Integer> seenChunks = null;
		long currentTag = -1;
		
		// queue up a reftag
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		
		// go through each chunk, ensuring we get every chunk of each page
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			long shortTag = ref.tag.shortTag();
			if(shortTag != currentTag) {
				// if we moved to a new page, and we had a previous page, we should have seen all its chunks
				if(seenChunks != null) assertEquals(numChunks, seenChunks.size());
				assertFalse(seenPageTags.contains(shortTag));
				seenChunks = new HashSet<Integer>();
				currentTag = shortTag;
			}
			
			// chunk index should be sensible, and we should not have seen this chunk for this page
			assertTrue(0 <= ref.index && ref.index < numChunks);
			assertFalse(seenChunks.contains(ref.index));
			seenPageTags.add(shortTag);
			seenChunks.add(ref.index);
		}
		
		// last page should have sent all its chunks, and we should have seen the expected number of pages + the page tree (1 page)
		assertEquals(numChunks, seenChunks.size());
		assertEquals(inode2Indirect.getRefTag().getNumPages() + 1, seenPageTags.size());
		
		// every page should actually be a part of the requested reftag
		PageTree tree = new PageTree(inode2Indirect);
		for(int i = 0; i < tree.numPages(); i++) {
			assertTrue(seenPageTags.contains(tree.getPageTag(i).shortTag()));
		}
	}
	
	@Test
	public void testAddInodeContentsHonorsPrioritySetting() {
		// queue up low-priority and high-priority reftags
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		queue.addInodeContents(1, revTag, inodeIndirect.getStat().getInodeId());
		assertTrue(queue.hasNextChunk());
		assertEquals(inodeIndirect.getRefTag().getStorageTag(), queue.nextChunk().tag);
		
		// try again, except go high-priority first then low-priority
		queue.stopAll();
		queue.addInodeContents(1, revTag, inodeIndirect.getStat().getInodeId());
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		assertTrue(queue.hasNextChunk());
		assertEquals(inodeIndirect.getRefTag().getStorageTag(), queue.nextChunk().tag);
	}
	
	@Test
	public void testAddInodeContentsAllowsReprioritization() {
		// queue up low-priority and high-priority reftags
		queue.addInodeContents(0, revTag, inodeIndirect.getStat().getInodeId());
		queue.addInodeContents(1, revTag, inode2Indirect.getStat().getInodeId());
		queue.nextChunk();
		
		// now make the low priority higher
		queue.addInodeContents(2, revTag, inodeIndirect.getStat().getInodeId());
		assertEquals(inodeIndirect.getRefTag().getStorageTag(), queue.nextChunk().tag);
	}
	
	@Test
	public void testAddInodeContentsAllowsCancellation() {
		// queue up low-priority and high-priority reftags
		queue.addInodeContents(0, revTag, inodeIndirect.getStat().getInodeId());
		queue.nextChunk();
		
		// now make the low priority higher
		queue.addInodeContents(Integer.MIN_VALUE, revTag, inodeIndirect.getStat().getInodeId());
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testAddInodeContentsSendsPagesInShuffledOrder() throws IOException {
		int n = 500;
		int[] matchCounts = new int[1024];
		for(int i = 0; i < n; i++) {
			// queue up a reftag, note the order the pages come in
			LinkedList<Long> seenPageTags = new LinkedList<Long>();
			queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
			while(queue.hasNextChunk()) {
				long shortTag = queue.nextChunk().tag.shortTag();
				if(seenPageTags.isEmpty() || seenPageTags.getLast() != shortTag) {
					seenPageTags.add(shortTag);
				}
			}
			
			// reshuffle, go again, note how many positions in the new lineup match the old one
			Shuffler.purgeFixedOrderings();
			queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
			int pagesSeen = 0, matches = 0;
			long lastShortTag = 0;
			while(queue.hasNextChunk()) {
				long shortTag = queue.nextChunk().tag.shortTag();
				if(lastShortTag != shortTag) {
					lastShortTag = shortTag;
					if(seenPageTags.get(pagesSeen) == shortTag) {
						matches++;
					}
					
					pagesSeen++;
				}
			}
			
			matchCounts[matches]++;
			assertTrue(pagesSeen == seenPageTags.size());
		}
		
		checkMatchRange(n, matchCounts);
	}
	
	@Test
	public void testAddInodeContentsToleratesNonexistentRevTag() throws IOException {
		// make a reftag for which we have no data
		byte[] corruptRaw = revTag.getRefTag().getBytes().clone();
		corruptRaw[2] ^= 0x20;
		RefTag corruptRef = new RefTag(archive, corruptRaw);
		RevisionTag corrupt = new RevisionTag(corruptRef, 0, 0, false);
		
		// queue it up; shouldn't generate any chunks, or problems
		queue.addInodeContents(0, corrupt, inodeIndirect.getStat().getInodeId());
		assertFalse(queue.hasNextChunk());
		
		// try again, except add a real one after and make sure we still get data
		queue.addInodeContents(0, corrupt, inodeIndirect.getStat().getInodeId());
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		assertQueueDrainOfSize((int) (numChunks*new PageTree(inode2Indirect).numDataPages()));
		
		// and again, except the legal one goes first
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		queue.addInodeContents(0, corrupt, inodeIndirect.getStat().getInodeId());
		assertQueueDrainOfSize((int) (numChunks*new PageTree(inode2Indirect).numDataPages()));
	}
	
	@Test
	public void testAddInodeContentsToleratesImmediateRefTag() {
		queue.addInodeContents(0, revTag, inodeImmediate.getStat().getInodeId());
		assertFalse(queue.hasNextChunk());

		queue.addInodeContents(0, revTag, inodeImmediate.getStat().getInodeId());
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		assertQueueDrainOfSize((int) (numChunks*new PageTree(inode2Indirect).numDataPages()));
		
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		queue.addInodeContents(0, revTag, inodeImmediate.getStat().getInodeId());
		assertQueueDrainOfSize((int) (numChunks*new PageTree(inode2Indirect).numDataPages()));
	}
	
	@Test
	public void testAddInodeContentsToleratesIndirectRefTag() {
		queue.addInodeContents(0, revTag, inodeIndirect.getStat().getInodeId());
		assertQueueDrainOfSize(numChunks);
	}
	
	@Test
	public void testAddInodeContentsToleratesDoublyIndirectRefTag() {
		long numDataPages = new PageTree(inode2Indirect).numDataPages();
		queue.addInodeContents(0, revTag, inode2Indirect.getStat().getInodeId());
		assertQueueDrainOfSize((int) (numChunks * numDataPages));
	}
	
	@Test
	public void testAddRevisionTagEnqueuesAllRefTagsInRevision() throws IOException {
		queue.addRevisionTag(0, revTag);
		expectTagsFromQueue(expectedPageTagsForRevTag(revTag));
		
		queue.addRevisionTag(0, secondRevTag);
		expectTagsFromQueue(expectedPageTagsForRevTag(secondRevTag));
	}
	
	@Test
	public void testAddRevisionTagHonorsPriority() throws IOException {
		assertFalse(expectedPageTagsForRevTag(secondRevTag).contains(pageTag.shortTag()));
		queue.addRevisionTag(0, secondRevTag);
		queue.addPageTag(1, pageTag);
		assertEquals(pageTag, queue.nextChunk().tag);
		
		queue.stopAll();
		queue.addRevisionTag(1, secondRevTag);
		queue.addPageTag(0, pageTag);
		
		StorageTag nextTag = queue.nextChunk().tag;
		assertNotEquals(pageTag, nextTag);
	}
	
	@Test
	public void testAddRevisionTagAllowsReprioritization() throws IOException {
		assertFalse(expectedPageTagsForRevTag(secondRevTag).contains(pageTag.shortTag()));
		queue.addRevisionTag(0, secondRevTag);
		queue.addPageTag(1, pageTag);
		queue.addRevisionTag(2, secondRevTag);
		assertNotEquals(pageTag, queue.nextChunk().tag);
		
		queue.stopAll();
		queue.addRevisionTag(2, secondRevTag);
		queue.addPageTag(1, pageTag);
		queue.addRevisionTag(0, secondRevTag);
		
		StorageTag nextTag = queue.nextChunk().tag;
		assertEquals(pageTag, nextTag);
	}
	
	@Test
	public void testAddRevisionTagAllowsCancellation() throws IOException {
		assertFalse(expectedPageTagsForRevTag(secondRevTag).contains(pageTag.shortTag()));
		queue.addRevisionTag(0, secondRevTag);
		queue.nextChunk();
		queue.addRevisionTag(Integer.MIN_VALUE, secondRevTag);
		assertFalse(queue.hasNextChunk());
	}
	
	public Inode inodeForPageTag(RevisionTag revTag, StorageTag pageTag) throws IOException {
		try(ZKFS fs = revTag.readOnlyFS()) {
			for(int i = 0; i < fs.getInodeTable().nextInodeId(); i++) {
				Inode inode = fs.getInodeTable().inodeWithId(i);
				PageTree tree = new PageTree(inode);
				for(int j = 0; j < tree.numPages(); j++) {
					if(pageTag.equals(tree.getPageTag(j))) {
						return inode;
					}
				}
	
				for(int j = 0; j < tree.numChunks(); j++) {
					if(pageTag.equals(tree.tagForChunk(j))) {
						return inode;
					}
				}
			}
		}
		
		fail();
		return null; // makes the compiler happy
	}
	
	@Test
	public void testAddRevisionTagSendsInodesInShuffledOrder() throws IOException {
		LinkedList<Long> seenInodes = new LinkedList<Long>();

		queue.addRevisionTag(0, secondRevTag);
		while(queue.hasNextChunk()) {
			Inode inode = inodeForPageTag(secondRevTag, queue.nextChunk().tag);
			if(seenInodes.isEmpty() || !seenInodes.getLast().equals(inode.getStat().getInodeId())) {
				seenInodes.add(inode.getStat().getInodeId());
			}
		}
		
		Shuffler.purgeFixedOrderings();
		long lastId = -1;
		int matches = 0, numTagsSeen = 0;
		queue.addRevisionTag(0, secondRevTag);
		while(queue.hasNextChunk()) {
			Inode inode = inodeForPageTag(secondRevTag, queue.nextChunk().tag);
			if(lastId < 0 || lastId != inode.getStat().getInodeId()) {
				if(seenInodes.get(numTagsSeen).equals(inode.getStat().getInodeId())) {
					matches++;
				}
				
				numTagsSeen++;
				lastId = inode.getStat().getInodeId();
			}
		}
		
		assertEquals(seenInodes.size(), numTagsSeen);
		assertTrue(matches <= numTagsSeen/2);
	}
	
	@Test
	public void testAddRevisionTagToleratesNonexistentRevisionTags() throws IOException {
		// make a revtag that isn't read
		byte[] corruptRaw = revTag.getRefTag().getBytes().clone();
		corruptRaw[2] ^= 0x20;
		RefTag corruptRef = new RefTag(archive, corruptRaw);
		RevisionTag corrupt = new RevisionTag(corruptRef, 0, 0, false);
		
		// queue it up; shouldn't generate any chunks, or problems
		queue.addRevisionTag(0, corrupt);
		assertFalse(queue.hasNextChunk());

		// try again, except add a real one after and make sure we still get data
		queue.addRevisionTag(0, corrupt);
		queue.addRevisionTag(0, revTag);
		expectTagsFromQueue(expectedPageTagsForRevTag(revTag));

		// and again, except the legal one goes first
		queue.addRevisionTag(0, revTag);
		queue.addRevisionTag(0, corrupt);
		expectTagsFromQueue(expectedPageTagsForRevTag(revTag));
	}
	
	@Test
	public void testAddRevisionTagToleratesFileRefTags() throws IOException {
		RefTag[] tags = { inodeImmediate.getRefTag(), inodeIndirect.getRefTag(), inode2Indirect.getRefTag() };
		for(RefTag tag : tags) {
			RevisionTag pageRevTag = new RevisionTag(tag, 0, 0, false);
			// queue it up with a file reftag; shouldn't generate any chunks, or problems
			queue.addRevisionTag(0, pageRevTag);
			assertFalse(queue.hasNextChunk());
			
			// try again, except add a real one after and make sure we still get data
			queue.addRevisionTag(0, pageRevTag);
			queue.addRevisionTag(0, revTag);
			expectTagsFromQueue(expectedPageTagsForRevTag(revTag));
			
			// and again, except the legal one goes first
			queue.addRevisionTag(0, revTag);
			queue.addRevisionTag(0, pageRevTag);
			expectTagsFromQueue(expectedPageTagsForRevTag(revTag));
		}
	}
	
	///////////
	
	@Test
	public void testAddRevisionTagForStructureEnqueuesAllStructuralRefTagsInRevision() throws IOException {
		queue.addRevisionTagForStructure(0, revTag);
		expectTagsFromQueue(expectedStructuralPageTagsForRevTag(revTag));
		
		queue.addRevisionTagForStructure(0, secondRevTag);
		expectTagsFromQueue(expectedStructuralPageTagsForRevTag(secondRevTag));
	}
	
	@Test
	public void testAddRevisionTagForStructureHonorsPriority() throws IOException {
		assertFalse(expectedPageTagsForRevTag(secondRevTag).contains(pageTag.shortTag()));
		queue.addRevisionTagForStructure(0, secondRevTag);
		queue.addPageTag(1, pageTag);
		assertTrue(pageTag.equals(queue.nextChunk().tag));
		
		queue.stopAll();
		queue.addRevisionTagForStructure(1, secondRevTag);
		queue.addPageTag(0, pageTag);
		
		StorageTag nextTag = queue.nextChunk().tag;
		assertNotEquals(pageTag, nextTag);
	}
	
	@Test
	public void testAddRevisionTagForStructureAllowsReprioritization() throws IOException {
		assertFalse(expectedPageTagsForRevTag(secondRevTag).contains(pageTag.shortTag()));
		queue.addRevisionTagForStructure(0, secondRevTag);
		queue.addPageTag(1, pageTag);
		queue.addRevisionTagForStructure(2, secondRevTag);
		assertNotEquals(pageTag, queue.nextChunk().tag);
		
		queue.stopAll();
		queue.addRevisionTagForStructure(2, secondRevTag);
		queue.addPageTag(1, pageTag);
		queue.addRevisionTagForStructure(0, secondRevTag);
		
		StorageTag nextTag = queue.nextChunk().tag;
		assertEquals(pageTag, nextTag);
	}
	
	@Test
	public void testAddRevisionTagForStructureAllowsCancellation() throws IOException {
		assertFalse(expectedStructuralPageTagsForRevTag(secondRevTag).contains(pageTag.shortTag()));
		queue.addRevisionTagForStructure(0, secondRevTag);
		queue.nextChunk();
		queue.addRevisionTagForStructure(Integer.MIN_VALUE, secondRevTag);
		assertFalse(queue.hasNextChunk());
	}
	
	// Disabled for now 
	@Test @Ignore
	public void testAddRevisionTagForStructureSendsInodesInShuffledOrder() throws IOException {
		LinkedList<Long> seenInodes = new LinkedList<Long>();

		queue.addRevisionTagForStructure(0, secondRevTag);
		while(queue.hasNextChunk()) {
			Inode inode = inodeForPageTag(secondRevTag, queue.nextChunk().tag);
			if(seenInodes.isEmpty() || !seenInodes.getLast().equals(inode.getStat().getInodeId())) {
				seenInodes.add(inode.getStat().getInodeId());
			}
		}
		
		Shuffler.purgeFixedOrderings();
		long lastId = -1;
		int matches = 0, numTagsSeen = 0;
		queue.addRevisionTagForStructure(0, secondRevTag);
		while(queue.hasNextChunk()) {
			Inode inode = inodeForPageTag(secondRevTag, queue.nextChunk().tag);
			if(lastId < 0 || lastId != inode.getStat().getInodeId()) {
				if(seenInodes.get(numTagsSeen).equals(inode.getStat().getInodeId())) {
					matches++;
				}
				
				numTagsSeen++;
				lastId = inode.getStat().getInodeId();
			}
		}
		
		assertEquals(seenInodes.size(), numTagsSeen);
		assertTrue(matches <= numTagsSeen/2);
	}
	
	@Test
	public void testAddRevisionTagForStructureToleratesNonexistentRevisionTags() throws IOException {
		// make a revtag that isn't read
		byte[] corruptRaw = revTag.getRefTag().getBytes().clone();
		corruptRaw[2] ^= 0x20;
		RefTag corruptRef = new RefTag(archive, corruptRaw);
		RevisionTag corrupt = new RevisionTag(corruptRef, 0, 0, false);
		
		// queue it up; shouldn't generate any chunks, or problems
		queue.addRevisionTagForStructure(0, corrupt);
		assertFalse(queue.hasNextChunk());

		// try again, except add a real one after and make sure we still get data
		queue.addRevisionTagForStructure(0, corrupt);
		queue.addRevisionTagForStructure(0, revTag);
		expectTagsFromQueue(expectedStructuralPageTagsForRevTag(revTag));

		// and again, except the legal one goes first
		queue.addRevisionTag(0, revTag);
		queue.addRevisionTag(0, corrupt);
		expectTagsFromQueue(expectedPageTagsForRevTag(revTag));
	}
	
	@Test
	public void testAddRevisionTagForStructureToleratesFileRefTags() throws IOException {
		RefTag[] tags = { inodeImmediate.getRefTag(), inodeIndirect.getRefTag(), inode2Indirect.getRefTag() };
		for(RefTag tag : tags) {
			RevisionTag pageRevTag = new RevisionTag(tag, 0, 0, false);
			// queue it up with a file reftag; shouldn't generate any chunks, or problems
			queue.addRevisionTagForStructure(0, pageRevTag);
			assertFalse(queue.hasNextChunk());
			
			// try again, except add a real one after and make sure we still get data
			queue.addRevisionTagForStructure(0, pageRevTag);
			queue.addRevisionTagForStructure(0, revTag);
			expectTagsFromQueue(expectedStructuralPageTagsForRevTag(revTag));
			
			// and again, except the legal one goes first
			queue.addRevisionTagForStructure(0, revTag);
			queue.addRevisionTagForStructure(0, pageRevTag);
			expectTagsFromQueue(expectedStructuralPageTagsForRevTag(revTag));
		}
	}
	
	@Test
	public void testStopAllEmptiesQueue() {
		queue.addRevisionTag(0, revTag);
		assertTrue(queue.hasNextChunk());
		queue.stopAll();
		assertFalse(queue.hasNextChunk());
		
		queue.addRevisionTag(0, revTag);
		assertTrue(queue.hasNextChunk());
		queue.nextChunk();
		queue.stopAll();
		assertFalse(queue.hasNextChunk());
		
		queue.startSendingEverything();
		assertTrue(queue.hasNextChunk());
		queue.stopAll();
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testExpectTagNextReturnsTrueIfPageTagIsNextItem() throws IOException {
		queue.addPageTag(0, pageTag);
		assertTrue(queue.expectTagNext(pageTag));
		
		queue.nextChunk();
		assertTrue(queue.expectTagNext(pageTag));
		
		queue.nextChunk();
		assertTrue(queue.expectTagNext(pageTag));
	}
	
	@Test
	public void testExpectTagNextReturnsFalseIfPageTagIsNotNextItem() throws IOException {
		queue.addPageTag(0, pageTag);
		queue.addRevisionTag(1, revTag);
		
		assertFalse(queue.expectTagNext(pageTag));
		queue.nextChunk();
		assertFalse(queue.expectTagNext(pageTag));
	}
	
	@Test
	public void testExpectTagNextReturnsFalseIfQueueIsEmpty() {
		assertFalse(queue.hasNextChunk());
		assertFalse(queue.expectTagNext(pageTag));
	}
	
	@Test
	public void testExpectChunksIncludeCorrectData() throws IOException {
		queue.addPageTag(0, pageTag);
		byte[] pageData = archive.getStorage().read(pageTag.path());
		int numChunks = (int) Math.ceil(((double) pageData.length)/PeerMessage.FILE_CHUNK_SIZE);
		int lastPageSize = pageData.length % PeerMessage.FILE_CHUNK_SIZE;
		
		while(queue.hasNextChunk()) {
			ChunkReference chunk = queue.nextChunk();
			assertEquals(pageTag, chunk.tag);
			
			int len = chunk.index == (numChunks - 1) ? lastPageSize : PeerMessage.FILE_CHUNK_SIZE; 
			byte[] expectedData = new byte[len];
			System.arraycopy(pageData, chunk.index*PeerMessage.FILE_CHUNK_SIZE, expectedData, 0, len);
			
			assertTrue(Arrays.equals(expectedData, chunk.getData()));
		}
	}
	
	@Test
	public void testCloseSetsClosed() {
		queue.close();
		assertTrue(queue.closed);
	}
	
	@Test
	public void testCloseWakesFromNextChunkBlock() throws InterruptedException {
		MutableBoolean woke = new MutableBoolean();
		Thread t = new Thread(()-> {
			queue.nextChunk();
			woke.setTrue();
		});
		
		t.start();
		assertFalse(Util.waitUntil(50, ()->woke.booleanValue()));
		queue.close();
		t.join(50);
		assertTrue(woke.booleanValue());
	}
}
