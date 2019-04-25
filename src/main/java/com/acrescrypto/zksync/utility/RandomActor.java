package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.commons.lang3.mutable.MutableLong;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.exceptions.ActUnavailableException;

public class RandomActor {
	public interface RandomActionCallback {
		void act() throws IOException;
	}
	
	public class RandomAction {
		RandomActionCallback callback;
		long weight;
		
		public RandomAction(long weight, RandomActionCallback callback) {
			this.callback = callback;
			this.weight = weight;
		}
		
		boolean attempt(MutableLong count) throws IOException {
			count.subtract(weight);
			if(count.longValue() <= 0) {
				callback.act();
				return true;
			}
			
			return false;
		}
	}
	
	protected LinkedList<RandomAction> actions = new LinkedList<>();
	long totalWeight = 0;
	PRNG prng;
	
	public RandomActor() {
		this(0);
	}

	public RandomActor(long nonce) {
		this(CryptoSupport.defaultCrypto().prng(Util.serializeLong(nonce)));
	}
	
	public RandomActor(PRNG prng) {
		this.prng = prng;
	}
	
	public void addAction(long weight, RandomActionCallback callback) {
		assert(weight >= 0);
		actions.add(new RandomAction(weight, callback));
		totalWeight += weight;
	}
	
	public void act() throws IOException {
		while(true) {
			try {
				attemptSingleAct();
				return;
			} catch(ActUnavailableException exc) {}
		}
	}
	
	public void attemptSingleAct() throws IOException {
		MutableLong rng = new MutableLong(prng.getLong(totalWeight));
		for(RandomAction act : actions) {
			if(act.attempt(rng)) return;
		}
	}
}
