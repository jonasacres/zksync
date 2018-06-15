package com.acrescrypto.zksync.net;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.zkfs.Page;
import com.acrescrypto.zksync.fs.zkfs.PageMerkle;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFSTest;
import com.acrescrypto.zksync.fs.zkfs.ZKMaster;
import com.acrescrypto.zksync.net.PageQueue.ChunkQueueItem;
import com.acrescrypto.zksync.net.PageQueue.ChunkReference;
import com.acrescrypto.zksync.utility.Shuffler;
import com.acrescrypto.zksync.utility.Util;

public class PageQueueTest {
	static ZKArchive archive;
	static ZKMaster master;
	static RefTag refTagImmediate, refTagIndirect, refTag2Indirect, revTag, secondRevTag;
	static LinkedList<RefTag> refTagsInRevision, refTagsInSecondRevision;
	static byte[] pageTag;
	static int numChunks;
	
	PageQueue queue;
	
	public static RefTag addFile(ZKFS fs, LinkedList<RefTag> list, String path, byte[] contents) throws IOException {
		fs.write(path, contents);
		RefTag tag = fs.inodeForPath(path).getRefTag();
		if(list != null) list.add(tag);
		return tag;
	}
	
	@BeforeClass
	public static void beforeAll() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "");
		numChunks = (int) Math.ceil((double) archive.getConfig().getPageSize()/PeerMessage.FILE_CHUNK_SIZE);
		
		refTagsInRevision = new LinkedList<RefTag>();
		refTagsInSecondRevision = new LinkedList<RefTag>();
		
		ZKFS fs = archive.openBlank();
		refTagImmediate = addFile(fs, refTagsInRevision, "immediate", new byte[archive.getCrypto().hashLength()-1]);
		refTagIndirect = addFile(fs, refTagsInRevision, "indirect", new byte[archive.getConfig().getPageSize()]);
		refTag2Indirect = addFile(fs, refTagsInRevision, "2indirect", new byte[10*archive.getConfig().getPageSize()]);
		revTag = fs.commit();
		
		RefTag refTag = addFile(fs, null, "sample", new byte[10*archive.getConfig().getPageSize()]);
		pageTag = new PageMerkle(refTag).getPageTag(1);
		fs.commit();
		fs = archive.openRevision(revTag);		
		
		byte[] ones = new byte[2*archive.getConfig().getPageSize()];
		for(int i = 0; i < ones.length; i++) ones[i] = 1;
		
		addFile(fs, refTagsInSecondRevision, "2indirect", ones);
		for(int i = 0; i < 64; i++) {
			addFile(fs, refTagsInSecondRevision, "filler"+i, new byte[archive.getConfig().getPageSize()]);
		}
		secondRevTag = fs.commit();
	}
	
	@Before
	public void beforeEach() {
		queue = new PageQueue(archive);
	}
	
	@After
	public void afterEach() {
		queue.close();
	}
	
	@AfterClass
	public static void afterAll() {
		archive.close();
		master.close();
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	public HashSet<Long> expectedPageTagsForRefTag(RefTag refTag) throws IOException {
		HashSet<Long> expectedTags = new HashSet<Long>();
		if(refTag.getRefType() == RefTag.REF_TYPE_IMMEDIATE) return expectedTags;
		
		PageMerkle merkle = new PageMerkle(refTag);
		for(int i = 0; i < merkle.numPages(); i++) {
			expectedTags.add(Util.shortTag(merkle.getPageTag(i)));
		}
		
		return expectedTags;
	}

	public HashSet<Long> expectedPageTagsForRevTag(RefTag revTag) throws IOException {
		HashSet<Long> expectedTags = new HashSet<Long>();
		ZKFS fs = revTag.readOnlyFS();
		
		expectedTags.add(Util.shortTag(fs.getInodeTable().getInode().getRefTag().getHash()));
		for(int i = 0; i < fs.getInodeTable().nextInodeId; i++) {
			expectedTags.addAll(expectedPageTagsForRefTag(fs.getInodeTable().inodeWithId(i).getRefTag()));
		}
		
		return expectedTags;
	}
	
	public void expectTagsFromQueue(HashSet<Long> expected) {
		HashSet<Long> seenTags = new HashSet<Long>();
		HashSet<Integer> seenChunks = null;
		long lastTag = 0;
		
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			long shortTag = Util.shortTag(ref.tag);
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
		
		assertTrue(seenTags.size() == expected.size());
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
	public void testHashChunkReturnsFalseIfQueueEmpty() {
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testHashChunkReturnsFalseIfQueueHasOnlyGarbage() {
		queue.addPageTag(0, 0); // non-existent page, can't expand into any chunks
		assertFalse(queue.itemsByPriority.isEmpty());
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testHashChunkReturnsTrueIfQueueHasChunkAtHead() {
		queue.addPageTag(0, pageTag);
		queue.unpackNextReference();
		assertTrue(queue.itemsByPriority.peek() instanceof ChunkQueueItem);
		assertTrue(queue.hasNextChunk());
	}
	
	@Test
	public void testHashChunkReturnsTrueIfQueueHasNonchunkAtHead() {
		queue.addPageTag(0, pageTag);
		assertFalse(queue.itemsByPriority.peek() instanceof ChunkQueueItem);
		assertTrue(queue.hasNextChunk());
	}
	
	@Test
	public void testNextChunkReturnsImmediatelyWhenAvailable() {
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
				queue.addPageTag(0, pageTag); // unblock the parent
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
			ChunkReference ref = queue.new ChunkReference(archive.getStorage(), pageTag, priority);
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
		String[] paths = archive.getStorage().opendir("/").listRecursive(Directory.LIST_OPT_OMIT_DIRECTORIES);
		HashSet<String> tags = new HashSet<String>();
		HashMap<String,Integer> seenCount = new HashMap<String,Integer>();
		
		for(String path : paths) {
			tags.add(path);
		}
		
		// send everything, note how many times we see each tag
		queue.startSendingEverything();		
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			String path = Page.pathForTag(ref.tag); // use paths since Strings fit nicely in the HashMap
			assertTrue(tags.contains(path)); // it's an error if we send something that's not a real tag
			seenCount.put(path, seenCount.getOrDefault(path, 0)+1);
		}
		
		// should have seen all the chunks of all the tags
		assertTrue(seenCount.size() == tags.size());
		for(int count : seenCount.values()) {
			assertEquals(numChunks, count);
		}
	}
	
	@Test
	public void testEverythingSendsInShuffledOrder() throws IOException {
		HashMap<Integer,Integer> matchCounts = new HashMap<>();
		
		for(int i = 0; i < 50; i++) {
			LinkedList<Long> seenTags = new LinkedList<Long>();
			
			// send everything, note the order we get tags in
			queue.startSendingEverything();
			while(queue.hasNextChunk()) {
				long shortTag = Util.shortTag(queue.nextChunk().tag);
				if(seenTags.isEmpty() || seenTags.peekLast() != shortTag) {
					seenTags.add(shortTag);
				}
			}
			
			// go again, count up how many times the same tag comes at the same position in the lineup
			queue.startSendingEverything();
			int pagesSeen = 0, matches = 0;
			long lastTag = -1;
			while(queue.hasNextChunk()) {
				long shortTag = Util.shortTag(queue.nextChunk().tag);
				if(lastTag == -1 || lastTag != shortTag) {
					lastTag = shortTag;
					if(seenTags.get(pagesSeen++) == shortTag) matches++;
				}
			}
			
			// should be mostly different
			matchCounts.put(matches, 1 + matchCounts.getOrDefault(matches, 0));
		}
		
		assertTrue(matchCounts.get(0) + matchCounts.get(1) >= 30);
		assertTrue(matchCounts.get(2) >= 4);
	}
	
	@Test
	public void testStopSendingEverything() {
		// ask for everything, read a chunk, ask to stop
		queue.startSendingEverything();
		byte[] tag = queue.nextChunk().tag;
		queue.stopSendingEverything();
		
		// it will finish sending the remaining chunks of the current page
		for(int i = 0; i < numChunks-1; i++) {
			assertTrue(queue.hasNextChunk());
			assertTrue(Arrays.equals(tag, queue.nextChunk().tag));
		}
		
		// and then nothing else
		assertFalse(queue.hasNextChunk());
	}
	
	@Test
	public void testStopSendingEverythingDoesntStopOtherRequests() {
		// ask for everything, read a chunk, ask to stop
		queue.startSendingEverything();
		do {} while(Arrays.equals(pageTag, queue.nextChunk().tag));
		queue.addPageTag(PageQueue.DEFAULT_EVERYTHING_PRIORITY-1, pageTag);
		queue.stopSendingEverything();
		
		// clear out the remainder of the current page
		for(int i = 0; i < numChunks-1; i++) {
			queue.nextChunk();
		}
		
		// but we have more data, and it's our requested page tag
		assertTrue(queue.hasNextChunk());
		assertTrue(Arrays.equals(pageTag, queue.nextChunk().tag));
	}
	
	@Test
	public void testSendEverythingPriority() {
		// ask for everything, and get a chunk so the queue has a bunch of chunks at the front at everything's priority
		// (also make sure that the chunk isn't for the page we're about to ask for)
		queue.startSendingEverything();
		do {} while(Arrays.equals(pageTag, queue.nextChunk().tag));
		
		// ask for a page at a priority slightly higher than everything
		queue.addPageTag(PageQueue.DEFAULT_EVERYTHING_PRIORITY+1, pageTag);
		
		// the next chunk should be from that page
		assertTrue(Arrays.equals(pageTag, queue.nextChunk().tag));
	}
	
	@Test
	public void testAddPageTagEnqueuesAllChunksInPage() {
		queue.addPageTag(0, pageTag);
		HashSet<Integer> seenIndexes = new HashSet<Integer>();
		
		// now make sure every chunk we get has the requested tag and a sensible index that we've not seen before
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			assertTrue(Arrays.equals(pageTag, ref.tag));
			assertTrue(0 <= ref.index);
			assertTrue(ref.index < numChunks);
			assertFalse(seenIndexes.contains(ref.index));
			seenIndexes.add(ref.index);
		}
		
		// we should have seen all the chunks
		assertEquals(numChunks, seenIndexes.size());
	}
	
	@Test
	public void testAddPageTagHonorsPrioritySettingBeforeChunking() {
		assertFalse(Arrays.equals(pageTag, refTagIndirect.getHash())); // nothing up my sleeves...
		// add a low-priority tag, then a high-priority one
		queue.addPageTag(0, pageTag);
		queue.addPageTag(1, refTagIndirect.getHash());
		
		// get the next chunk -- should be for the high-priority request
		assertTrue(queue.hasNextChunk());
		assertTrue(Arrays.equals(refTagIndirect.getHash(), queue.nextChunk().tag));
		
		// wipe the slate and start over, this time with high-priority first
		queue.stopAll();
		queue.addPageTag(1, pageTag);
		queue.addPageTag(0, refTagIndirect.getHash());
		assertTrue(queue.hasNextChunk());
		assertTrue(Arrays.equals(pageTag, queue.nextChunk().tag));
	}
	
	@Test
	public void testAddPageTagHonorsPrioritySettingAfterChunking() {
		assertFalse(Arrays.equals(pageTag, refTagIndirect.getHash()));
		// add a low-priority tag, then ask for a chunk to get its chunks queued up
		queue.addPageTag(0, pageTag);
		queue.nextChunk();
		
		// now add a high-priority request. the next chunk should be for that request.
		queue.addPageTag(1, refTagIndirect.getHash());
		assertTrue(queue.hasNextChunk());
		assertTrue(Arrays.equals(refTagIndirect.getHash(), queue.nextChunk().tag));
	}
	
	@Test
	public void testAddPageTagSendsChunksInShuffledOrder() {
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
		assertTrue(matchCounts[0] + matchCounts[1] > 2*numIterations/3);
		assertTrue(Math.abs(((double) matchCounts[1])/matchCounts[2] - 2.0) < 0.5);
		assertTrue(Math.abs(((double) matchCounts[2])/matchCounts[3] - 3.0) < 1);
	}
	
	@Test
	public void testAddPageTagToleratesNonexistentPages() {
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
	public void testAddRefTagContentsEnqueuesAllPagesInRefTag() throws IOException {
		HashSet<Long> seenPageTags = new HashSet<Long>();
		HashSet<Integer> seenChunks = null;
		long currentTag = -1;
		
		// queue up a reftag
		queue.addRefTagContents(0, refTag2Indirect);
		
		// go through each chunk, ensuring we get every chunk of each page
		while(queue.hasNextChunk()) {
			ChunkReference ref = queue.nextChunk();
			long shortTag = Util.shortTag(ref.tag);
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
		
		// last page should have sent all its chunks, and we should have seen the expected number of pages
		assertEquals(numChunks, seenChunks.size());
		assertEquals(refTag2Indirect.getNumPages(), seenPageTags.size());
		
		// every page should actually be a part of the requested reftag
		PageMerkle merkle = new PageMerkle(refTag2Indirect);
		for(int i = 0; i < merkle.numPages(); i++) {
			assertTrue(seenPageTags.contains(Util.shortTag(merkle.getPageTag(i))));
		}
	}
	
	@Test
	public void testAddRefTagContentsHonorsPrioritySetting() {
		// queue up low-priority and high-priority reftags
		queue.addRefTagContents(0, refTag2Indirect);
		queue.addRefTagContents(1, refTagIndirect);
		assertTrue(queue.hasNextChunk());
		assertTrue(Arrays.equals(refTagIndirect.getHash(), queue.nextChunk().tag));
		
		// try again, except go high-priority first then low-priority
		queue.stopAll();
		queue.addRefTagContents(1, refTagIndirect);
		queue.addRefTagContents(0, refTag2Indirect);
		assertTrue(queue.hasNextChunk());
		assertTrue(Arrays.equals(refTagIndirect.getHash(), queue.nextChunk().tag));
	}
	
	@Test
	public void testAddRefTagContentsSendsPagesInShuffledOrder() {
		HashMap<Integer,Integer> matchCounts = new HashMap<>();
		for(int i = 0; i < 50; i++) {
			// queue up a reftag, note the order the pages come in
			LinkedList<Long> seenPageTags = new LinkedList<Long>();
			queue.addRefTagContents(0, refTag2Indirect);
			while(queue.hasNextChunk()) {
				long shortTag = Util.shortTag(queue.nextChunk().tag);
				if(seenPageTags.isEmpty() || seenPageTags.getLast() != shortTag) {
					seenPageTags.add(shortTag);
				}
			}
			
			// reshuffle, go again, note how many positions in the new lineup match the old one
			Shuffler.purgeFixedOrderings();
			queue.addRefTagContents(0, refTag2Indirect);
			int pagesSeen = 0, matches = 0;
			long lastShortTag = 0;
			while(queue.hasNextChunk()) {
				long shortTag = Util.shortTag(queue.nextChunk().tag);
				if(lastShortTag != shortTag) {
					lastShortTag = shortTag;
					if(seenPageTags.get(pagesSeen) == shortTag) {
						matches++;
					}
					
					pagesSeen++;
				}
			}
			
			matchCounts.put(matches, 1 + matchCounts.getOrDefault(matches, 0));
			assertTrue(pagesSeen == seenPageTags.size());
		}
		
		assertTrue(matchCounts.get(0) + matchCounts.get(1) >= 30);
		assertTrue(matchCounts.get(2) >= 4);
	}
	
	@Test
	public void testAddRefTagContentsToleratesNonexistentRefTag() {
		// make a reftag for which we have no data
		byte[] corruptRaw = refTag2Indirect.getBytes().clone();
		corruptRaw[2] ^= 0x20;
		RefTag corrupt = new RefTag(archive, corruptRaw);
		
		// queue it up; shouldn't generate any chunks, or problems
		queue.addRefTagContents(0, corrupt);
		assertFalse(queue.hasNextChunk());
		
		// try again, except add a real one after and make sure we still get data
		queue.addRefTagContents(0, corrupt);
		queue.addRefTagContents(0, refTag2Indirect);
		assertQueueDrainOfSize((int) (numChunks*refTag2Indirect.getNumPages()));
		
		// and again, except the legal one goes first
		queue.addRefTagContents(0, refTag2Indirect);
		queue.addRefTagContents(0, corrupt);
		assertQueueDrainOfSize((int) (numChunks*refTag2Indirect.getNumPages()));
	}
	
	@Test
	public void testAddRefTagContentsToleratesImmediateRefTag() {
		queue.addRefTagContents(0, refTagImmediate);
		assertFalse(queue.hasNextChunk());

		queue.addRefTagContents(0, refTagImmediate);
		queue.addRefTagContents(0, refTag2Indirect);
		assertQueueDrainOfSize((int) (numChunks*refTag2Indirect.getNumPages()));
		
		queue.addRefTagContents(0, refTag2Indirect);
		queue.addRefTagContents(0, refTagImmediate);
		assertQueueDrainOfSize((int) (numChunks*refTag2Indirect.getNumPages()));
	}
	
	@Test
	public void testAddRefTagContentsToleratesIndirectRefTag() {
		queue.addRefTagContents(0, refTagIndirect);
		assertQueueDrainOfSize(numChunks);
	}
	
	@Test
	public void testAddRefTagContentsToleratesDoublyIndirectRefTag() {
		queue.addRefTagContents(0, refTag2Indirect);
		assertQueueDrainOfSize((int) (numChunks * refTag2Indirect.getNumPages()));
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
		assertFalse(expectedPageTagsForRevTag(secondRevTag).contains(Util.shortTag(pageTag)));
		queue.addRevisionTag(0, secondRevTag);
		queue.addPageTag(1, pageTag);
		assertTrue(Arrays.equals(pageTag, queue.nextChunk().tag));
		
		queue.stopAll();
		queue.addRevisionTag(1, secondRevTag);
		queue.addPageTag(0, pageTag);
		
		byte[] nextTag = queue.nextChunk().tag;
		assertFalse(Arrays.equals(pageTag, nextTag));
	}
	
	public RefTag refTagForPageTag(RefTag revTag, byte[] pageTag) throws IOException {
		ZKFS fs = revTag.readOnlyFS();
		for(int i = 0; i < fs.getInodeTable().nextInodeId; i++) {
			RefTag refTag = fs.getInodeTable().inodeWithId(i).getRefTag();
			PageMerkle merkle = new PageMerkle(refTag);
			for(int j = 0; j < merkle.numPages(); j++) {
				if(Arrays.equals(pageTag, merkle.getPageTag(j))) {
					return refTag;
				}
			}
		}
		
		fail();
		return null; // makes the compiler happy
	}
	
	@Test
	public void testAddRevisionTagSendsRefTagsInShuffledOrder() throws IOException {
		LinkedList<RefTag> seenRefTags = new LinkedList<RefTag>();

		queue.addRevisionTag(0, secondRevTag);
		while(queue.hasNextChunk()) {
			RefTag tag = refTagForPageTag(secondRevTag, queue.nextChunk().tag);
			if(seenRefTags.isEmpty() || !seenRefTags.getLast().equals(tag)) {
				seenRefTags.add(tag);
			}
		}
		
		Shuffler.purgeFixedOrderings();
		RefTag lastTag = null;
		int matches = 0, numTagsSeen = 0;
		queue.addRevisionTag(0, secondRevTag);
		while(queue.hasNextChunk()) {
			RefTag tag = refTagForPageTag(secondRevTag, queue.nextChunk().tag);
			if(lastTag == null || !lastTag.equals(tag)) {
				if(seenRefTags.get(numTagsSeen).equals(tag)) {
					matches++;
				}
				
				numTagsSeen++;
				lastTag = tag;
			}
		}
		
		assertEquals(seenRefTags.size(), numTagsSeen);
		assertTrue(matches <= numTagsSeen/2);
	}
	
	@Test
	public void testAddRevisionTagToleratesNonexistentRevisionTags() throws IOException {
		// make a revtag that isn't read
		byte[] corruptRaw = revTag.getBytes().clone();
		corruptRaw[2] ^= 0x20;
		RefTag corrupt = new RefTag(archive, corruptRaw);
		
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
		RefTag[] tags = { refTagImmediate, refTagIndirect, refTag2Indirect };
		for(RefTag tag : tags) {
			// queue it up with a file reftag; shouldn't generate any chunks, or problems
			queue.addRevisionTag(0, tag);
			assertFalse(queue.hasNextChunk());
			
			// try again, except add a real one after and make sure we still get data
			queue.addRevisionTag(0, tag);
			queue.addRevisionTag(0, revTag);
			expectTagsFromQueue(expectedPageTagsForRevTag(revTag));
			
			// and again, except the legal one goes first
			queue.addRevisionTag(0, revTag);
			queue.addRevisionTag(0, tag);
			expectTagsFromQueue(expectedPageTagsForRevTag(revTag));
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
	public void testExpectTagNextReturnsTrueIfPageTagIsNextItem() {
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
		byte[] pageData = archive.getStorage().read(Page.pathForTag(pageTag));
		int numChunks = (int) Math.ceil(((double) pageData.length)/PeerMessage.FILE_CHUNK_SIZE);
		int lastPageSize = pageData.length % PeerMessage.FILE_CHUNK_SIZE;
		
		while(queue.hasNextChunk()) {
			ChunkReference chunk = queue.nextChunk();
			assertTrue(Arrays.equals(pageTag, chunk.tag));
			
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
