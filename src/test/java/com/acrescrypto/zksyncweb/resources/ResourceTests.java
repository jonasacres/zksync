package com.acrescrypto.zksyncweb.resources;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksyncweb.resources.archive.ArchiveResourceTests;
import com.acrescrypto.zksyncweb.resources.blacklist.BlacklistResourceTests;
import com.acrescrypto.zksyncweb.resources.dht.DHTResourceTests;
import com.acrescrypto.zksyncweb.resources.log.LogResourceTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	ArchiveResourceTests.class,
	BlacklistResourceTests.class,
	GlobalResourceTest.class,
	LogResourceTest.class,
	VersionResourceTest.class,
	DHTResourceTests.class,
})

public class ResourceTests {

}
