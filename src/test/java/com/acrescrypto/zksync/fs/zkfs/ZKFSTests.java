package com.acrescrypto.zksync.fs.zkfs;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetResolverTest;
import com.acrescrypto.zksync.fs.zkfs.resolver.DiffSetTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({
	RevisionTreeTest.class,
	ZKFSTest.class,
	ZKDirectoryTest.class,
	ZKFileTest.class,
	FreeListTest.class,
	DiffSetTest.class,
	DiffSetResolverTest.class,
	ObfuscatedRefTagTest.class,
	StoredAccessRecordTest.class,
	StoredAccessTest.class,
	ArchiveAccessor.class
})

public class ZKFSTests {

}
