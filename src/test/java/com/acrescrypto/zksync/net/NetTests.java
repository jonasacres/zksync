package com.acrescrypto.zksync.net;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	BlacklistEntryTest.class,
	BlacklistTest.class,
	ChunkAccumulatorTest.class,
	PageQueueTest.class,
	PeerMessageIncomingTest.class,
	PeerMessageOutgoingTest.class,
	PeerSwarmTest.class,
})

public class NetTests {
}
