package com.acrescrypto.zksync;

import static org.junit.Assert.fail;

import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang3.mutable.MutableInt;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.FSMirror;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFile;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.utility.SnoozeThread;
import com.acrescrypto.zksync.utility.SnoozeThreadSupervisor;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksync.utility.WaitSupervisor;
import com.acrescrypto.zksync.utility.WaitSupervisor.WaitTask;
import com.dosse.upnp.UPnP;

public class TestUtils {
	static HashSet<Long> knownZombieThreads = new HashSet<>();
	static HashSet<SnoozeThread> knownSnoozeThreads = new HashSet<>();
	static HashSet<WaitTask> knownWaitTasks = new HashSet<>();
	static HashSet<ZKFS> knownZombieFilesystems = new HashSet<>();
	static int knownZombieWatches = 0, knownOpenFiles = 0;
	
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
		
		/* Commenting this block out in case the issue noted below reappears, but on JDK 11.0.2 there
		 * appears to be no issue with stale cache worker threads when running UniversalTests as of
		 * 99d77cb (testing May 2019).
		 * 
		 * Delete this entirely in 2020 if the issue does not reappear.
		// Since moving to JDK11, cached worker threads seem to wait 60s to close. Ugh! Just tolerate those...
		if(backtrace.length > 0 && backtrace[0].getClassName().equals("jdk.internal.misc.Unsafe")) {
			return true;
		} */
		
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
		return threadsTidy() && FSMirror.numActive() <= knownZombieWatches && FS.getGlobalOpenFiles().size() == 0;
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
					/* This is easy to turn off since it can be quite noisy.  */
					
					boolean enableNoisyOutput = true;
					if(enableNoisyOutput) {
						System.out.println(Util.dumpStackTrace(traces.get(t), 1));
						for(StackTraceElement element : t.getStackTrace()) {
							System.out.println("\t" + element);
						}
					}
				}
			}
			
			FS.getGlobalOpenFiles().forEach((file, backtrace)->{
				System.out.printf("Open file: %d [%s] %s -- %s\nOpened from:\n%s",
						(file instanceof ZKFile) ? ((ZKFile) file).getRetainCount() : -1,
						file.getFs(),
						System.identityHashCode(file),
						file.getPath(),
						Util.dumpStackTrace(backtrace.getStackTrace(), 1));
			});
			
			ZKFS.getOpenInstances().forEach((zkfs, backtrace)->{
				if(knownZombieFilesystems.contains(zkfs)) return;
				knownZombieFilesystems.add(zkfs);
				System.out.printf("Open ZKFS: %s\n\tOpen from:\n%s",
						System.identityHashCode(zkfs),
						Util.dumpStackTrace(backtrace.getStackTrace(), 2));
				
				System.out.printf("\t%d open files:\n", zkfs.getOpenFiles().size());
				MutableInt i = new MutableInt(0);
				zkfs.getOpenFiles().forEach((file, trace)->{
					i.increment();
					System.out.printf("\t\t#%3d %s\n", i.getValue(), file.getPath());
				});
				
				System.out.printf("\t%d total retentions, %d active:\n",
						zkfs.getRetentions().size(),
						zkfs.getRetainCount());
				i.setValue(0);
				
				zkfs.getRetentions().forEach((trace)->{
					i.increment();
					System.out.printf("\t\tRetention %d\n%s",
							i.getValue(),
							Util.dumpStackTrace(trace.getStackTrace(), 3));
				});

				System.out.printf("\t%d total closures:\n",
						zkfs.getClosures().size());
				i.setValue(0);
				
				zkfs.getClosures().forEach((trace)->{
					i.increment();
					System.out.printf("\t\tClosure %d\n%s",
							i.getValue(),
							Util.dumpStackTrace(trace.getStackTrace(), 3));
				});
			});
			
			knownZombieWatches = FSMirror.numActive();
			knownOpenFiles = FS.getGlobalOpenFiles().size();
			
			// System.out.println("Thread untidiness detected!");
			// Util.threadReport(true);
			System.out.println("FS telemetry enabled: " + FS.fileHandleTelemetryEnabled);
			System.out.println("Active FS monitors: " + FSMirror.numActive());
			System.out.println("Open file handles: " + FS.getGlobalOpenFiles().size());
			System.out.println("Open ZKFS instances: " + ZKFS.getOpenInstances().size());
			fail();
		}
		
		System.gc();
		ConfigDefaults.resetDefaults();
	}
	
	public static void stopDebugMode() {
		CryptoSupport.cheapArgon2     = false;
		FS.fileHandleTelemetryEnabled = false;
		UPnP.disableDebug();
		ConfigDefaults.resetDefaults();
		System.gc();
	}

	public static void startDebugMode() {
		CryptoSupport.cheapArgon2     = true;
		FS.fileHandleTelemetryEnabled = true;
		UPnP.enableDebug();
		ConfigDefaults.resetDefaults();
		ConfigDefaults.getActiveDefaults().set("net.dht.bootstrap.peerfile", "");
		System.gc();
	}
	
	public static int testHttpPort() {
	    return 42512;
	}
	
	public static String testHttpUrl() {
	    return "http://127.0.0.1:" + testHttpPort();
	}
}
