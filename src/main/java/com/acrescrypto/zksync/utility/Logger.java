package com.acrescrypto.zksync.utility;

import java.sql.Timestamp;

public class Logger {
	// "Problem" here means something in the program or the environment it runs in is preventing intended operation.
	public final static int LOG_DEBUG = 0;     // not worth recording unless we think something's wrong
	public final static int LOG_INFO = 1;      // routine status info; the most trivial stuff still worth logging in normal operation
	public final static int LOG_NOTICE = 2;    // big stuff a troubleshooter definitely needs to know, but isn't likely to be a problem
	public final static int LOG_SECURITY = 3;  // security-related event (protocol violations, blacklist additions, etc.)
	public final static int LOG_WARN = 4;      // might be a problem, might be a one-off. 
	public final static int LOG_ERROR = 5;     // problem! shouldn't appear at runtime unless something is wrong.
	public final static int LOG_FATAL = 6;  // we're about to crash/fail spectacularly, and these are our last words
	
	public static void debug(String msg) { log(LOG_DEBUG, msg); }
	public static void info(String msg) { log(LOG_INFO, msg); }
	public static void notice(String msg) { log(LOG_NOTICE, msg); }
	public static void security(String msg) { log(LOG_SECURITY, msg); }
	public static void warn(String msg) { log(LOG_WARN, msg); }
	public static void fatal(String msg) { log(LOG_ERROR, msg); }
	public static void exception(Exception exc) { exception(LOG_ERROR, exc); }
	
	protected static int level = LOG_INFO;
	protected static Logger singleton = new Logger();

	public static void exception(int level, Exception exc) { log(level, exc.toString()); }
	
	public static String levelName(int level) {
		String[] names = { "DEBUG", "INFO", "NOTICE", "SECURITY", "WARN", "ERROR", "FATAL" };
		assert(0 <= level && level < names.length);
		return names[level];
	}
	
	public static void log(int level, String msg) {
		String timestamp = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Timestamp(System.currentTimeMillis()));
		System.out.println(timestamp + " " + levelName(level) + ": " + msg);
	}
}
