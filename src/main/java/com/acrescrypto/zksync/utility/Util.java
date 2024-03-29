package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.HashContext;
import com.acrescrypto.zksync.crypto.PublicDHKey;
import com.acrescrypto.zksync.fs.zkfs.RefTag;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;

public class Util {
	static long debugTime = -1;
	static long launchTime = -1;
	
	private final static char[] hexArray = "0123456789abcdef".toCharArray();
	private final static Logger logger = LoggerFactory.getLogger(Util.class);
	
	public interface WaitTest {
		boolean test();
	}
	
	public interface ProgressiveWaitTest {
		int test();
	}
	
	public interface AnonymousCallback {
		void cb() throws Exception;
	}
	
	public static void hexdump(String caption, byte[] data) {
		hexdump(caption, data, 0, data.length);
	}
	
	public static synchronized void hexdump(String caption, byte[] data, int offset, int len) {
		System.out.println(hexdumpStr(caption, data, offset, len) + "\n\n");
	}
	
	public static String hexdumpStr(String caption, byte[] data) {
		return hexdumpStr(caption, data, 0, data.length);
	}
	
	public static String hexdumpStr(String caption, byte[] data, int offset, int len) {
		LinkedList<String> lines = new LinkedList<>();
		if(data == null) {
			return String.format("%s (null, no fingerprint)", caption);
		}

		lines.add(String.format("%s (%d bytes, fingerprint %s)", caption, len, fingerprint(data, offset, len)));
		
		String line = "";
		for(int i = 0; i <= 16 * (int) Math.ceil((double) len/16); i++) {
			if((i % 16) == 0) {
				if(i != 0) {
					line += "  |";
					for(int j = 0; j < 16; j++) {
						byte b = i+j-16 < len ? data[offset+i+j-16] : 0;
						char c = '.';
						if(b >= 0x20 && b < 0x7f) c = (char) b;
						line += String.format("%c", c);
					}
					
					line += "|";
					lines.add(line);
					line = "";
				}

				line += String.format("%04x  ", i);
			}

			if(i < len) line += String.format("%02x", data[offset+i]);
			else line += "  ";
			if((i % 2) == 1) line += " ";
			if((i % 8) == 7) line += " ";
		}
		
		lines.add(line);
		return String.join("\n", lines);
	}

	public static String fingerprint(byte[] data, int offset, int len) {
		return bytesToHex((new HashContext()).update(data, offset, len).finish()).substring(0, 8);
	}

	public static byte[] hexToBytes(String s) {
		// credit: https://stackoverflow.com/questions/140131/convert-a-string-representation-of-a-hex-dump-to-a-byte-array-using-java/140861#140861
		try {
			int len = s.getBytes().length;
			byte[] data = new byte[len / 2];
			for (int i = 0; i < len; i += 2) {
				data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
						+ Character.digit(s.charAt(i+1), 16));
			}
			return data;
		} catch(Exception exc) {
			logger.error("Unable to decode hex string: {}", s, exc);
			throw exc;
		}
	}

	public static String bytesToHex(byte[] b) {
		return bytesToHex(b, b.length);
	}
	
	public static String bytesToHex(byte[] b, int length) {
		// credit: https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java#9855338
		int len = Math.min(length, b.length);
	    char[] hexChars = new char[2*len];
	    for(int j = 0; j < len; j++) {
	        int v = b[j] & 0xff;
	        hexChars[2*j] = hexArray[v >>> 4];
	        hexChars[2*j + 1] = hexArray[v & 0x0f];
	    }
	    return new String(hexChars);
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
		return System.getProperty("os.name").toLowerCase().contains("windows");
	}

	public static boolean isOSX() {
		return System.getProperty("os.name").equals("Mac OS X");
	}
	
	public static boolean isSuperuser() {
		if(isLinux()) {
			// not reliable, but at the moment we're only using this to decide whether to test FS stuff
			return System.getProperty("user.name").equals("root");
		} else {
			return false;
		}
	}
	
	public static boolean waitUntil(int maxDelayMs, WaitTest test) {
		long endTime = maxDelayMs <= 0 ? Long.MAX_VALUE : System.currentTimeMillis() + maxDelayMs;
		boolean passed = false;
		while(System.currentTimeMillis() < endTime && !passed) {
			passed = test.test();
			if(!passed) {
				sleep(1);
			}
		}
		
		return passed || System.currentTimeMillis() < endTime;
	}
	
	public static boolean waitProgressively(int maxDelayMs, int targetValue, ProgressiveWaitTest test) {
		Integer value = null;
		long expireTime = System.currentTimeMillis() + maxDelayMs;
		
		while(value == null || value.intValue() < targetValue) {
			if(System.currentTimeMillis() > expireTime) return false;
			int newValue = test.test();
			if(value == null || newValue > value.intValue()) {
				expireTime = System.currentTimeMillis() + maxDelayMs;
				value = newValue;
			}
		}
		
		return true;
	}
	
	public static void blockOnPoll(WaitTest test) {
		while(test.test()) {
			sleep(1);
		}
	}
	
	public static void delay(long delay, AnonymousCallback action) {
		new SnoozeThread(delay, false, ()->{try { action.cb(); } catch(Exception exc) {}}).hashCode();
	}
	
	public static void sleep(long durationMs) {
		if(durationMs <= 0) return;
		try {
			Thread.sleep(durationMs);
		} catch(InterruptedException exc) {}
	}
	
	public static void ensure(long maxDelayMs, long frequency, WaitTest test, AnonymousCallback action) {
		WaitSupervisor.shared().add(maxDelayMs, frequency, test, action);
	}
	
	/** Side-channel-attack resistant comparison of byte arrays. */
	public static boolean safeEquals(byte[] a, byte[] b) {
		if(a.length != b.length) return false;
		return safeEquals(a, b, a.length);
	}
	
	public static boolean safeEquals(byte[] a, byte[] b, int length) {
		byte d = 0;
		if(a.length < length || b.length < length) return false;
		for(int i = 0; i < length; i++) {
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
	
	public static ArrayList<Map<String, Object>> threadReport() {
		ArrayList<Map<String, Object>> report = new ArrayList<>();
		Map<Thread,StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
		
		for(Thread t : stackTraces.keySet()) {
			StackTraceElement[] elements = stackTraces.get(t);
			HashMap<String,Object> threadInfo = new HashMap<>();
			ArrayList<HashMap<String,Object>> traceList = new ArrayList<>(elements.length);
			
			ThreadGroup group = t.getThreadGroup();
			HashMap<String,Object> groupMap = new HashMap<>(), currentGroupMap = groupMap;
			while(group != null) {
				currentGroupMap.put("name", group.getName());
				group = group.getParent();
				if(group != null) {
					HashMap<String, Object> parentGroupMap = new HashMap<>();
					currentGroupMap.put("parent", parentGroupMap);
					currentGroupMap = parentGroupMap;
				} else {
					currentGroupMap.put("parent", null);
					currentGroupMap = null;
				}
			}
			
			for(StackTraceElement e : stackTraces.get(t)) {
				HashMap<String, Object> elementMap = new HashMap<>();
				elementMap.put("class", e.getClassName());
				elementMap.put("method", e.getMethodName());
				elementMap.put("file", e.getFileName());
				elementMap.put("line", e.getLineNumber());
				traceList.add(elementMap);
			}
			
			threadInfo.put("name", t.getName());
			threadInfo.put("priority", t.getPriority());
			threadInfo.put("id", t.getId());
			threadInfo.put("group", groupMap);
			threadInfo.put("state", t.getState().toString());
			threadInfo.put("trace", traceList);
			
			report.add(threadInfo);
		}
		
		return report;
	}
	
	public static String caller(int depth) {
		try {
			StackTraceElement e = (new Throwable()).getStackTrace()[2+depth];
			return e.getClassName() + "." + e.getMethodName() + ":" + e.getLineNumber();
		} catch(ArrayIndexOutOfBoundsException exc) {
			return "";
		}
	}
	
	public static byte[] concat(byte[]... values) {
		int totalSize = 0;
		for(byte[] value : values) totalSize += value.length;
		
		ByteBuffer buf = ByteBuffer.allocate(totalSize);
		for(byte[] value : values) buf.put(value);
		
		return buf.array();
	}
	
	public static int compareArrays(byte[] a, byte[] b) {
		int m = Math.min(a.length, b.length);
		for(int i = 0; i < m; i++) {
			if(a[i] == b[i]) continue;
			if(Util.unsignByte(a[i]) < Util.unsignByte(b[i])) return -1;
			return 1;
		}
		
		if(a.length == b.length) return 0;
		if(a.length < b.length) return -1;
		return 1;
	}
	
	public static void setThreadName(String name) {
		Thread.currentThread().setName(name + " " + String.format("%08x", System.identityHashCode(Thread.currentThread())));
	}
	
	public static byte[] decode64(String base64) {
		return Base64.getDecoder().decode(fromWebSafeBase64(base64));
	}
	
	public static String encode64(byte[] data) {
		return Base64.getEncoder().encodeToString(data);
	}
	
	public static String fromWebSafeBase64(String base64) {
		return base64.replaceAll("\\.", "+").replaceAll("_", "/");
	}
	
	public static String toWebSafeBase64(String base64) {
		return base64.replace('+', '.').replace('/', '_');
	}

	public static byte[] zero(byte[] array) {
		for(int i = 0; i < array.length; i++) {
			array[i] = 0;
		}
		
		return null;
	}
	
	public static long launchTime() {
		if(launchTime < 0) launchTime = System.currentTimeMillis();
		return launchTime;
	}
	
	public static String formatArchiveId(byte[] archiveId) {
		return "a-" + formatLongIdB64(archiveId);
	}
	
	public static String formatRevisionTag(RevisionTag rev) {
		if(rev == null || rev.getBytes() == null) return "rev-?-?-null";
		if(rev.isUnpacked()) {
			return "rev:" + rev.getHeight() + "-" + formatLongIdB64(rev.getBytes());
		} else {
			return "rev-?-?-" + formatLongIdB64(rev.getBytes());
		}
	}
	
	public static String formatPubKey(PublicDHKey key) {
		return "pk-" + formatLongId(key.getBytes());
	}
	
	public static String formatLongId(byte[] longId) {
		return Util.bytesToHex(longId, 4);
	}
	
	public static String formatLongIdB64(byte[] longId) {
		return Util.toWebSafeBase64(Util.encode64(longId)).substring(0, 8);
	}

	public static String formatRefTag(RefTag refTag) {
		try {
			if(refTag.getStorageTag().isFinalized()) {
				return String.format("ref-%d-%s",
						refTag.getRefType(),
						Util.bytesToHex(refTag.getBytes(), 4));
			} else {
				return String.format("ref-%d-pending", refTag.getRefType());
			}
		} catch(IOException exc) {
			exc.printStackTrace();
			throw new RuntimeException(exc);
		} catch(NullPointerException exc) {
			return "ref-null";
		}
	}

	public static String strhash(byte[] result) {
		return Util.bytesToHex(CryptoSupport.defaultCrypto().hash(result), 8) + "-" + result.length;
	}
	
	public static String dumpStackTrace(StackTraceElement[] trace) {
		return dumpStackTrace(trace, 0);
	}
	
	public static String dumpStackTrace(StackTraceElement[] trace, int depth) {
		int m = 0;
		String s = "";
		int longest = 0;
		for(StackTraceElement e : trace) {
			String f = e.getFileName();
			if(f != null && f.length() > longest) {
				longest = f.length();
			}
		}
		
		for(StackTraceElement e : trace) {
			m += 1;
			s += String.format("%s#%2d    %"+longest+"s:%4d %s\n",
					"\t".repeat(depth),
					m,
					e.getFileName(),
					e.getLineNumber(),
					e.getMethodName());
		}
		
		return s;
	}
	
	public static void debugLog(String message) {
		logger.debug(message);
	}
}
