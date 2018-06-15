package com.acrescrypto.zksync.utility;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;

import org.junit.AfterClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.utility.Shuffler.ShuffleOrdering;

public class ShufflerTest {
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testShuffleOrderingCreatesRandomOrderings() {
		for(int size = 10; size < 15; size++) {
			ShuffleOrdering[] orderings = new ShuffleOrdering[2];
			for(int i = 0; i < 2; i++) {
				orderings[i] = new ShuffleOrdering(size);
			}
			
			int matches = 0;
			for(int j = 0; j < size; j++) {
				if(orderings[0].order[j] == orderings[1].order[j]) matches++;
			}
			
			assertFalse(matches >= 8);
		}
	}
	
	@Test
	public void testShufflerConstructorMakesOrderings() {
		for(int size = 10; size < 20; size++) {
			Shuffler shuffler = new Shuffler(size);
			assertEquals(shuffler.ordering.order.length, size);
		}
	}
	
	@Test
	public void testFixedShufflerRoundsSizeUp() {
		int threshold = (int) Math.pow(Shuffler.FIXED_MAX_INEFFICIENCY, 2);
		assertEquals(Shuffler.fixedShuffler(threshold-1).ordering.order.length, Shuffler.fixedShuffler(threshold).ordering.order.length);
		assertEquals(Shuffler.fixedShuffler(threshold).ordering.order.length, threshold);
		assertEquals(Shuffler.fixedShuffler(threshold+1).ordering.order.length, Shuffler.fixedShuffler(threshold+2).ordering.order.length);
		assertEquals(Shuffler.fixedShuffler(threshold+1).ordering.order.length, (int) Shuffler.FIXED_MAX_INEFFICIENCY*threshold);
	}
	
	@Test
	public void testNextReturnsInShuffledOrder() {
		int size = 20, drawn = 0;
		Shuffler shuffler = new Shuffler(size);
		while(shuffler.hasNext()) {
			int draw = shuffler.next();
			assertEquals(draw, shuffler.ordering.order[drawn]);
			drawn++;
		}
		
		assertEquals(drawn, size);
	}
	
	@Test
	public void testNextDoesNotExceedRange() {
		int size = 20, drawn = 0;
		HashSet<Integer> seen = new HashSet<Integer>();
		Shuffler shuffler = Shuffler.fixedShuffler(size);
		while(shuffler.hasNext()) {
			int draw = shuffler.next();
			assertFalse(seen.contains(draw));
			assertTrue(0 <= draw && draw < size);
			seen.add(draw);
			drawn++;
		}
		
		assertEquals(drawn, size);
	}
}
