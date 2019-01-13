package com.acrescrypto.zksync;

import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Map;

import com.acrescrypto.zksync.fs.zkfs.FSMirror;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.SnoozeThreadSupervisor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksync.utility.WaitSupervisor;
import com.acrescrypto.zksync.utility.WaitSupervisor.WaitTask;

public class TestUtils {
	static HashSet<Long> knownZombieThreads = new HashSet<>();
	static HashSet<SnoozeThread> knownSnoozeThreads = new HashSet<>();
	static HashSet<WaitTask> knownWaitTasks = new HashSet<>();
	static int knownZombieWatches = 0;
	
	public static boolean isThreadAcceptable(Thread thread, StackTraceElement[] backtrace) {
		if(knownZombieThreads.contains(thread.getId())) return true;
		
		String[] acceptable = {
				"Reference Handler",
				"process reaper",
				"main",
				"Signal Dispatcher",
				"Finalizer",
				"ReaderThread",
				"SnoozeThreadSupervisor",
				"SnoozeThreadSupervisor idle worker",
				"WaitSupervisor",
				"WaitSupervisor idle worker",
				"Idle worker"
		};
		
		for(String name : acceptable) {
			if(thread.getName().equals(name)) return true;
		}
		
		// Since moving to JDK11, cached worker threads seem to wait 60s to close. Ugh! Just tolerate those...
		if(backtrace.length > 0 && backtrace[0].getClassName().equals("jdk.internal.misc.Unsafe")) {
			return true;
		}
		
		return false;
	}
	
	public static boolean threadsTidy() {
		Map<Thread,StackTraceElement[]> traces = Thread.getAllStackTraces();
		boolean success = true;
		
		synchronized(SnoozeThreadSupervisor.shared()) {
			for(SnoozeThread snoozeThread : SnoozeThreadSupervisor.shared().getItems()) {
				if(knownSnoozeThreads.contains(snoozeThread) || snoozeThread.isCancelled()) continue;
				knownSnoozeThreads.add(snoozeThread);
				success = false;
			}
		}
		
		for(WaitTask waitTask : WaitSupervisor.shared().getTasks()) {
			if(knownWaitTasks.contains(waitTask)) continue;
			knownWaitTasks.add(waitTask);
			success = false;
		}
		
		for(Thread t : traces.keySet()) {
			if(!isThreadAcceptable(t, traces.get(t))) {
				knownZombieThreads.add(t.getId());
				success = false;
			}
		}
		
		return success;
	}
	
	public static boolean isTidy() {
		return threadsTidy() && FSMirror.numActive() <= knownZombieWatches;
	}
	
	public static void assertTidy() {
		SnoozeThreadSupervisor.shared().prune(false);
		// This is starting to get a bit ITFy... already had to bump the tolerance to 5000ms from 1000ms and lower.
		if(!Util.waitUntil(10000, ()->isTidy())) {
			Map<Thread,StackTraceElement[]> traces = Thread.getAllStackTraces();
			knownZombieThreads.clear();
			for(Thread t : traces.keySet()) {
				if(!isThreadAcceptable(t, traces.get(t))) {
					System.out.println("Unacceptable thread: " + t);
					for(StackTraceElement element : t.getStackTrace()) {
						System.out.println("\t" + element);
					}
				}
			}
			
			knownZombieWatches = FSMirror.numActive();
			
			// System.out.println("Thread untidiness detected!");
			// Util.threadReport(true);
			System.out.println("Active FS monitors: " + FSMirror.numActive());
			fail();
		}
	}
}
