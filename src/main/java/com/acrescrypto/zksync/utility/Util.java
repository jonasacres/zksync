package com.acrescrypto.zksync.utility;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.acrescrypto.zksync.crypto.HashContext;

public class Util {
	static long debugTime = -1;
	
	public interface WaitTest {
		boolean test();
	}
	
	public interface AnonymousCallback {
		void cb() throws Exception;
	}
	
	public static synchronized void hexdump(String caption, byte[] data) {
		System.out.printf("%s (%d bytes, fingerprint %s)\n", caption, data.length, fingerprint(data));
		for(int i = 0; i <= 16 * (int) Math.ceil((double) data.length/16); i++) {
			if((i % 16) == 0) {
				if(i != 0) {
					System.out.print("  |");
					for(int j = 0; j < 16; j++) {
						byte b = i+j-16 < data.length ? data[i+j-16] : 0;
						char c = '.';
						if(b >= 0x20 && b < 0x80) c = (char) b;
						System.out.printf("%c", c);
					}
					System.out.println("|");
				}

				System.out.printf("%04x  ", i);
			}

			if(i < data.length) System.out.printf("%02x", data[i]);
			else System.out.print("  ");
			if((i % 2) == 1) System.out.print(" ");
			if((i % 8) == 7) System.out.print(" ");
		}

		System.out.println();
		System.out.println();
	}

	public static String fingerprint(byte[] data) {
		return bytesToHex((new HashContext(data)).finish()).substring(0, 8);
	}

	public static byte[] hexToBytes(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
					+ Character.digit(s.charAt(i+1), 16));
		}
		return data;
	}

	public static String bytesToHex(byte[] b) {
		return bytesToHex(b, b.length);
	}
	
	public static String bytesToHex(byte[] b, int length) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < length; i++) sb.append(String.format("%02x", b[i]));
		return sb.toString();
	}

	public static byte[] truncateArray(byte[] array, int length) {
		byte[] newArray = new byte[length];
		for(int i = 0; i < length; i++) newArray[i] = array[i];
		return newArray;
	}
	
	public static long unsignInt(int intVal) {
		return intVal >= 0 ? intVal : intVal + 0x100000000l;
	}
	
	public static int unsignShort(short shortVal) {
		return shortVal >= 0 ? shortVal : shortVal + 0x10000;
	}
	
	public static short unsignByte(byte byteVal) {
		return (short) (byteVal >= 0 ? byteVal : byteVal + 0x100);
	}
	
	public static long shortTag(byte[] tag) {
		return ByteBuffer.wrap(tag).getLong();
	}

	public static boolean isLinux() {
		return System.getProperty("os.name").equals("Linux");
	}

	public static boolean isWindows() {
		return false; // TODO: implement
	}

	public static boolean isOSX() {
		return System.getProperty("os.name").equals("Mac OS X");
	}
	
	public static boolean waitUntil(int maxDelay, WaitTest test) {
		long endTime = maxDelay <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + maxDelay;
		while(System.currentTimeMillis() < endTime && !test.test()) {
			try {
				Thread.sleep(1);
			} catch(InterruptedException exc) {}
		}
		
		return System.currentTimeMillis() < endTime;
	}
	
	public static void blockOn(WaitTest test) {
		while(test.test()) {
			sleep(1);
		}
	}
	
	public static void delay(long delay, AnonymousCallback action) {
		new Thread(()->{
			Thread.currentThread().setName("Util.delay thread");
			sleep(delay);
			try { action.cb(); } catch (Exception e) {}
		}).start();
	}
	
	public static void sleep(long durationMs) {
		if(durationMs <= 0) return;
		try {
			Thread.sleep(durationMs);
		} catch(InterruptedException exc) {}
	}
	
	public static void ensure(int maxDelay, WaitTest test, AnonymousCallback action) {
		new Thread(()-> {
			Thread.currentThread().setName("Util.ensure thread " + action.getClass().getSimpleName());
			if(waitUntil(maxDelay, test)) return;
			try { action.cb(); } catch(Exception exc) {}
		}).start();
	}
	
	/** Side-channel-attack resistant comparison of byte arrays. */
	public static boolean safeEquals(byte[] a, byte[] b) {
		byte d = 0;
		if(a.length != b.length) return false;
		for(int i = 0; i < a.length; i++) {
			d |= (a[i] ^ b[i]);
		}
		
		return d == 0;
	}

	public static long currentTimeNanos() {
		if(debugTime < 0) return 1000l*1000l*System.currentTimeMillis();
		return debugTime;
	}
	
	public static long currentTimeMillis() {
		return currentTimeNanos()/(1000l*1000l);
	}
	
	public static void setCurrentTimeNanos(long time) {
		debugTime = time;
	}

	public static void setCurrentTimeMillis(long time) {
		setCurrentTimeNanos(time*1000l*1000l);		
	}
	
	public static <T> T min(Collection<T> collection, Comparator<T> comp) {
		T result = null;
		for(T item : collection) {
			if(result == null || comp.compare(item, result) < 0) {
				result = item;
			}
		}
		
		return result;
	}
	
	public static <T> T max(Collection<T> collection, Comparator<T> comp) {
		T result = null;
		for(T item : collection) {
			if(result == null || comp.compare(item, result) > 0) {
				result = item;
			}
		}
		
		return result;
	}
	
	public static byte[] serializeShort(short x) {
		return ByteBuffer.allocate(2).putShort(x).array();
	}

	public static byte[] serializeInt(int x) {
		return ByteBuffer.allocate(4).putInt(x).array();
	}
	
	public static byte[] serializeLong(long x) {
		return ByteBuffer.allocate(8).putLong(x).array();
	}
	
	public static void threadReport(boolean includeStackTraces) {
		Map<Thread,StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
		HashMap<String,Integer> instanceCounts = new HashMap<>();
		
		System.out.println("Thread count: " + stackTraces.size());
		for(Thread t : stackTraces.keySet()) {
			instanceCounts.put(t.getName(), instanceCounts.getOrDefault(t.getName(), 0) + 1);
			if(includeStackTraces) {
				System.out.println(t.getName() + " - " + t.getId());
				for(StackTraceElement e : stackTraces.get(t)) {
					System.out.println("\t"+e.getClassName() + "::" + e.getMethodName() + " line " + e.getLineNumber());
				}
				System.out.println("\n");
			}
		}
		
		ArrayList<String> sortedInstanceNames = new ArrayList<>();
		sortedInstanceNames.addAll(instanceCounts.keySet());
		sortedInstanceNames.sort((a, b)->instanceCounts.get(b).compareTo(instanceCounts.get(a)));
		
		for(String name : sortedInstanceNames) {
			System.out.println(name + ": " + instanceCounts.get(name));
		}
		
		System.out.println("\n");
	}
	
	public static String caller(int depth) {
		try {
			StackTraceElement e = (new Throwable()).getStackTrace()[2+depth];
			return e.getClassName() + "." + e.getMethodName() + ":" + e.getLineNumber();
		} catch(ArrayIndexOutOfBoundsException exc) {
			return "";
		}
	}
}
