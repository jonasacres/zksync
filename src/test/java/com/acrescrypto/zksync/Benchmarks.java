package com.acrescrypto.zksync;

import static org.junit.Assert.fail;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.crypto.CryptoBenchmark;
import com.acrescrypto.zksync.exceptions.BenchmarkFinishedException;
import com.acrescrypto.zksync.fs.FSBenchmarks;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	FSBenchmarks.class,
	CryptoBenchmark.class
})

public class Benchmarks {
	public interface BenchmarkTest {
		void run(int iteration) throws Exception;
	}
	
	public final static int DEFAULT_TEST_INTERVAL_MS = 3000;
	
	public static void beginBenchmarkSuite(String description) {
		String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"));
		output(timestamp + " Benchmarking " + description);
	}
	
	public static int run(String unit, int interval, BenchmarkTest test) {
		long startTs = System.currentTimeMillis(), endTs = startTs + interval;
		int numUnits = 0;
		while(System.currentTimeMillis() < endTs) {
			try {
				test.run(numUnits);
			} catch(BenchmarkFinishedException exc) {
				break;
			} catch(Exception exc) {
				exc.printStackTrace();
				fail();
			}
			numUnits++;
		}
		
		outputResult(unit, numUnits, (int) (System.currentTimeMillis()-startTs));
		return numUnits;
	}
	
	public static int run(String unit, BenchmarkTest test) {
		return run(unit, DEFAULT_TEST_INTERVAL_MS, test);
	}
	
	public static void outputResult(String unit, int numUnits, int duration) {
		String caller = Thread.currentThread().getStackTrace()[4].getMethodName();
		double rate = 1000.0 * ((double) numUnits) / duration;
		output(String.format("\t%50s: %.03f %s/s (%d %s in %d ms)", caller, rate, unit, numUnits, unit, duration));
	}
	
	public static void finishBenchmarkSuite() {
		output("=====");
		output("");
	}
	
	public static void output(String message) {
		System.out.println(message);
	}
}
