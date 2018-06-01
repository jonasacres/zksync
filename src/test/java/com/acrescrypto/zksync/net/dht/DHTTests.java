package com.acrescrypto.zksync.net.dht;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	DHTIDTest.class,
	DHTBucketTest.class,
	DHTPeerTest.class,
	DHTAdvertisementRecordTest.class,
	DHTRecordStoreTest.class
})

public class DHTTests {
}
