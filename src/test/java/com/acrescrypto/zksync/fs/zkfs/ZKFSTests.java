package com.acrescrypto.zksync.fs.zkfs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.fs.zkfs.config.ConfigTests;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolverTest;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	RevisionListTest.class,
	BlockTest.class,
	BlockManagerTest.class,
	ZKFSTest.class,
	ZKDirectoryTest.class,
	ZKFileTest.class,
	FreeListTest.class,
	DiffSetTest.class,
	DiffSetResolverTest.class,
	StoredAccessRecordTest.class,
	StoredAccessTest.class,
	ArchiveAccessorTest.class,
	
    ZKArchiveConfigTest.class,
	ZKArchiveTest.class,
	
	ZKMasterTest.class,
	PageTest.class,
	PageTreeChunkTest.class,
	PageTreeTest.class,
	RefTagTest.class,
	RevisionTagTest.class,
	RevisionTreeTest.class,
	ZKFSManagerTest.class,
	FSMirrorTest.class,
	ConfigTests.class
})

public class ZKFSTests {

}
