package com.acrescrypto.zksync.net.dht;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	DHTIDTest.class,
	DHTBucketTest.class,
	DHTPeerTest.class,
	DHTRoutingTableTest.class,
	DHTAdvertisementRecordTest.class,
	DHTRecordStoreTest.class,
	DHTMessageTest.class,
	DHTMessageStubTest.class,
	DHTSearchOperationTest.class,
	DHTClientTest.class,
	DHTRecordTest.class,
	DHTZKArchiveDiscoveryTest.class
})

public class DHTTests {
}
