package com.acrescrypto.zksync;

import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Map;

import com.acrescrypto.zksync.utility.Util;

public class TestUtils {
	static HashSet<Long> knownZombieThreads = new HashSet<>();
	
	public static boolean isThreadAcceptable(Thread thread, StackTraceElement[] backtrace) {
		if(knownZombieThreads.contains(thread.getId())) return true;
		
		String[] acceptable = {
				"Reference Handler",
				"process reaper",
				"main",
				"Signal Dispatcher",
				"Finalizer",
				"ReaderThread"
		};
		
		for(String name : acceptable) {
			if(thread.getName().equals(name)) return true;
		}
		
		return false;
	}
	
	public static boolean threadsTidy() {
		Map<Thread,StackTraceElement[]> traces = Thread.getAllStackTraces();
		for(Thread t : traces.keySet()) {
			if(!isThreadAcceptable(t, traces.get(t))) {
				knownZombieThreads.add(t.getId());
				return false;
			}
		}
		
		return true;
	}
	
	public static boolean isTidy() {
		return threadsTidy();
	}
	
	public static void assertTidy() {
		if(!Util.waitUntil(500, ()->isTidy())) {
			Util.threadReport(true);
			fail();
		}
	}
}
