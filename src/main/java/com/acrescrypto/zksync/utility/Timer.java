package com.acrescrypto.zksync.utility;

import java.util.HashMap;

public class Timer {
	protected static HashMap<String,Timer> timers = new HashMap<String,Timer>();
	
	String name;
	long totalTime;
	int count;
	long start;
	
	public static void start(String name) {
		timers.putIfAbsent(name, new Timer(name));
		timers.get(name).start();
	}
	
	public static void stop(String name) {
		timers.get(name).end();
	}
	
	public static void dump() {
		for(Timer timer : timers.values()) {
			System.out.println(timer);
		}
	}
	
	public Timer(String name) {
		this.name = name;
		start = -1;
	}
	
	public void start() {
		if(start >= 0) return;
		start = System.nanoTime();
	}
	
	public void end() {
		totalTime += System.nanoTime() - start;
		count++;
		start = -1;
	}
	
	public String toString() {
		return String.format("%30s - %.03fms, %d invocations (avg. %.03fms) %s",
				name,
				totalTime/(1000.0*1000.0),
				count,
				totalTime/(1000.0*1000.0*count),
				start > 0 ? String.format("RUNNING %.03f", (System.nanoTime()-start)/(1000.0*1000.0)) : "");
	}
}
