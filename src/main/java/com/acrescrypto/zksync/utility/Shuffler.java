package com.acrescrypto.zksync.utility;

import java.io.IOException;

public class Shuffler {
	/** x > 1, where at most 1 in x entries in a fixed shuffling will be out of bounds.
	 * Higher value increases CPU utilization to reduce memory consumption. 
	 */
	public final static double FIXED_MAX_INEFFICIENCY = 2.0;
	
	public static class ShuffleOrdering {
		static HashCache<Integer,ShuffleOrdering> fixedOrderings = new HashCache<Integer,ShuffleOrdering>(32, (size)->new ShuffleOrdering(size), (size, order)->{});
		
		static ShuffleOrdering fixedOrdering(int size) {
			try {
				return fixedOrderings.get(size);
			} catch(IOException exc) {
				throw new RuntimeException();
			}
		}
		
		static void purgeFixedOrderings() {
			try {
				fixedOrderings.removeAll();
			} catch (IOException e) {
				throw new RuntimeException();
			}
		}
		
		public int[] order;
		
		public ShuffleOrdering(int size) {
			order = new int[size];
			for(int i = 0; i < size; i++) {
				int j = (int) ((i+1)*Math.random());
				if(i != j) {
					order[i] = order[j];
				}
				order[j] = i;
			}
		}
	}
	
	public static void purgeFixedOrderings() {
		ShuffleOrdering.purgeFixedOrderings();
	}
	
	public static Shuffler fixedShuffler(int size) {
		int roundSize = (int) Math.pow(FIXED_MAX_INEFFICIENCY, Math.ceil(Math.log(size)/Math.log(FIXED_MAX_INEFFICIENCY)));
		return new Shuffler(ShuffleOrdering.fixedOrdering(roundSize), size);
	}
	
	protected int index, size, drawn;
	protected ShuffleOrdering ordering;
	
	public Shuffler(int size) {
		this(new ShuffleOrdering(size), size);
	}
	
	public Shuffler(ShuffleOrdering ordering, int size) {
		assert(ordering.order.length >= size);
		this.ordering = ordering;
		this.size = size;
	}
	
	public int next() {
		try {
			while(ordering.order[index] >= size) index++;
			drawn++;
			return ordering.order[index++];
		} catch(ArrayIndexOutOfBoundsException exc) {
			return -1;
		}
	}

	public boolean hasNext() {
		return drawn < size;
	}
}
