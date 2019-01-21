package com.acrescrypto.zksync.utility;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	ShufflerTest.class,
	AppendableInputStreamTest.class,
	BandwidthMonitorTest.class,
	BandwidthAllocatorTest.class,
	MemLogAppenderTest.class
})

public class UtilityTests {

}
