package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;

public class RevisionTreeTest {
	CryptoSupport crypto;
	ZKMaster master;
	ZKArchiveConfig config;
	ZKArchive archive;
	RevisionTree tree;
	
	@BeforeClass
	public static void beforeAll() {
		ZKFSTest.cheapenArgon2Costs();
	}
	
	@Before
	public void beforeEach() throws IOException {
		crypto = new CryptoSupport();
		master = ZKMaster.openBlankTestVolume();
		archive = master.createDefaultArchive();
		config = archive.config;
		tree = config.revisionTree;
	}
	
	@After
	public void afterEach() {
		config.close();
		master.close();
	}
	
	@AfterClass
	public void afterAll() {
		ZKFSTest.restoreArgon2Costs();
		TestUtils.assertTidy();
	}
	
	// constructor loads existing tree if exists
	// constructor scans existing revision list if no tree found
	// constructor scans existing revision list if tree unreadable
	// constructor tolerates empty revision list
	
	// validateParentList rejects invalid parent lists for single parent revisions
	// validateParentList rejects empty parent lists for single parent revisions
	// validateParentList rejects subset parent lists for single parent revisions
	// validateParentList rejects superset parent lists for single parent revisions

	// validateParentList rejects invalid parent lists for multiparent revisions
	// validateParentList rejects empty parent lists for multiparent revisions
	// validateParentList rejects subset parent lists for multiparent revisions
	// validateParentList rejects superset parent lists for multiparent revisions
	
	// addParentsForTag validates tag parents
	// addParentsForTag tolerates multiple calls
	// addParentsForTag adds parent mapping
	// addParentsForTag automatically writes when autowrite enabled
	// addParentsForTag does not automatically write when autowrite not enabled
	// addParentsForTag unblocks waits for tag
	
	// scanRevTag does not block for missing revtags
	
	// hasParentsForTag returns true if parents known for revtag
	// hasParentsForTag returns false if parents not known for revtag
	
	// parentsForTag returns list of parents for tag
	// parentsForTag blocks until list obtained
	// parentsForTag returns after timeout
	// parentsForTag returns null if list not obtained before timeout
	// parentsForTag blocks without limit if timeout is negative
	
	// parentsForTagNonblocking returns null if tag not present
	// parentsForTagNonblocking returns parents if tag present
	
	// commonAncestor returns greatest-height, lowest-value shared ancestor between revisions
	//   case: one revision, blank
	//   case: one revision, one parent
	//   case: one revision, root
	//   case: two revisions, one parent
	//   case: two revisions, one shared parent, two distinct
	//   case: two revisions, multiple shared parents
	//   case: two revisions, shared grandparent
	//   case: two revisions, no shared ancestry
	//   case: many revisions, shared parent
	//   case: many revisions, multiple partial matches, shared grandparent
	//   case: many revisions, no shared ancestry
	//   case: many revisions, distinct heights
	
	// descendentOf returns true if revtag has indicated ancestor
	//   case: revtag and ancestor are the same
	//   case: ancestor is blank
	//   case: ancestor is immediate parent in single-parent list
    //   case: ancestor is immediate parent in multi-parent list
    //   case: ancestor is grandparent
    //   case: ancestor is great-grandparent

	// descendentOf returns false if revtag does not have indicated ancestor
	//   case: revtag is blank
	//   case: root revtag
	//   case: non-root revtag
}
