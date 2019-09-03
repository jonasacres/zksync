package com.acrescrypto.zksyncweb.resources.archive;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksyncweb.resources.archive.net.ArchiveNetResourceTests;
import com.acrescrypto.zksyncweb.resources.archive.revision.ArchiveRevisionResourceTests;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	ArchivesResourceTest.class,
	ArchiveResourceTest.class,
	ArchiveFsResourceTest.class,
	ArchiveNetResourceTests.class,
	ArchiveRevisionResourceTests.class,
})

public class ArchiveResourceTests {

}
