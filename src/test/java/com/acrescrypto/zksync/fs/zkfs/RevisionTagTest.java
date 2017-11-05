package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.*;

import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class RevisionTagTest {
	ZKFS fs;
	Revision rev, parent;
	RevisionTag tag;
	
	@Before
	public void beforeEach() throws IOException {
		if(fs != null) return;
		
		ZKFSTest.cheapenArgon2Costs();
		Security.addProvider(new BouncyCastleProvider());
		fs = new ZKFS(new LocalFS("/tmp/revision-tag-test"), "zksync".toCharArray());
		fs.getInodeTable().getInode().getStat().setUser("zksync");
		parent = fs.commit();
		
		fs = new ZKFS(fs.getStorage(), "zksync".toCharArray(), parent);
		rev = fs.commit();
		tag = rev.getTag();
	}
	
	@AfterClass
	public static void afterClass() {
		ZKFSTest.restoreArgon2Costs();
	}
	
	@Test
	public void testSerialization() throws IOException {
		RevisionTag deserialized = new RevisionTag(fs, tag.getPath());
		assertTrue(Arrays.equals(tag.getTag(), deserialized.getTag()));
		assertEquals(tag.authorHash, deserialized.authorHash);
		assertEquals(tag.flags, deserialized.flags);
		assertEquals(tag.parentShortTag, deserialized.parentShortTag);
		assertEquals(tag.timestamp, deserialized.timestamp);
		assertTrue(Arrays.equals(tag.keySalt, deserialized.keySalt));
	}
	
	@Test
	public void testAuthor() {
		byte[] authorHash = tag.refKey().authenticate(fs.getInodeTable().getStat().getUser().getBytes());
		long authorHashShort = ByteBuffer.wrap(authorHash).getLong();
		assertEquals(authorHashShort, tag.authorHash);
	}
	
	@Test
	public void testTimestamp() {
		assertEquals(System.currentTimeMillis()*1000l, tag.timestamp, 10);
	}
	
	@Test
	public void testParentTag() {
		assertEquals(ByteBuffer.wrap(parent.tag.getTag()).getLong(), tag.getParentShortTag());
	}
	
	@Test
	public void testRefKey() {
		assertTrue(Arrays.equals(fs.deriveKey(ZKFS.KEY_TYPE_AUTH, ZKFS.KEY_INDEX_REVISION_TREE).getRaw(), tag.refKey().getRaw()));
	}
	
	// TODO: ensure serialized tag complies with spec
}
