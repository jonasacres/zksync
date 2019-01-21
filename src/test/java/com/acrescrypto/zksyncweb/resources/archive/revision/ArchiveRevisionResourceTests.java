package com.acrescrypto.zksyncweb.resources.archive.revision;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	ArchiveRevisionsResourceTest.class,
	ArchiveRevisionResourceTest.class,
	ArchiveRevisionFsResourceTest.class,
	ArchiveRevisionActiveResourceTest.class,
	ArchiveRevisionLatestResourceTest.class,
})

public class ArchiveRevisionResourceTests {

}
