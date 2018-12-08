package com.acrescrypto.zksync.net;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.net.noise.NoiseTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	NoiseTests.class,
	BlacklistEntryTest.class,
	BlacklistTest.class,
	ChunkAccumulatorTest.class,
	PageQueueTest.class,
	PeerMessageIncomingTest.class,
	PeerMessageOutgoingTest.class,
	PeerSwarmTest.class,
	TCPPeerAdvertisementTest.class,
	TCPPeerAdvertisementListenerTest.class,
	TCPPeerSocketListenerTest.class,
	TCPPeerSocketTest.class,
	PeerConnectionTest.class,
	RequestPoolTest.class,
})

public class NetTests {
}
