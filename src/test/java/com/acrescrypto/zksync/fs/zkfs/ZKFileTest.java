package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.Security;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.fs.FileTestBase;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.localfs.LocalFS;

public class ZKFileTest extends FileTestBase {
	ZKFS zkscratch;
	
	@Before
	public void beforeEach() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		ZKFSTest.deleteFiles();
		LocalFS storage = new LocalFS(ZKFSTest.SCRATCH_DIR);
		java.io.File scratchDir = new java.io.File(ZKFSTest.SCRATCH_DIR);
		try {
			FileUtils.deleteDirectory(scratchDir);
		} catch (IOException e) {}
		(new java.io.File(ZKFSTest.SCRATCH_DIR)).mkdirs();

		scratch = zkscratch = ZKFS.fsForStorage(storage, "zksync".toCharArray());
	}
	
	@After
	public void afterEach() {
		ZKFSTest.restoreArgon2Costs();
	}

	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@Test
	public void testImmedateWrite() throws IOException {
		byte[] contents = new byte[zkscratch.archive.crypto.hashLength()-1];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		ZKFile file = zkscratch.open("immediate-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();
		
		file = zkscratch.open("immediate-write-test", ZKFile.O_RDONLY);
		assertTrue(Arrays.equals(file.read(), contents));
		assertEquals(RefTag.REF_TYPE_IMMEDIATE, file.inode.refTag.refType);
	}
	
	@Test
	public void testSinglePageWrite() throws IOException {
		byte[] contents = new byte[zkscratch.archive.getPrivConfig().getPageSize()];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		ZKFile file = zkscratch.open("singlepage-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();
		
		file = zkscratch.open("singlepage-write-test", ZKFile.O_RDONLY);
		assertTrue(Arrays.equals(file.read(), contents));
		assertEquals(RefTag.REF_TYPE_INDIRECT, file.inode.refTag.refType);
	}
	
	@Test
	public void testMultipageWrite() throws IOException {
		byte[] contents = new byte[5*zkscratch.archive.privConfig.getPageSize()];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		ZKFile file = zkscratch.open("multipage-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();
		
		file = zkscratch.open("multipage-write-test", ZKFile.O_RDONLY);
		assertTrue(Arrays.equals(file.read(), contents));
		assertEquals(RefTag.REF_TYPE_2INDIRECT, file.inode.refTag.refType);
	}
	
	@Test
	public void testPageTimestampSquashing() throws IOException {
		byte[] contents = new byte[5*zkscratch.archive.privConfig.getPageSize()];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		
		ZKFile file = zkscratch.open("multipage-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();

		for(int i = 0; i < Math.ceil(((double) file.getStat().getSize())/file.fs.archive.privConfig.getPageSize()); i++) {
			Page page = new Page(file, i);
			String path = ZKArchive.DATA_DIR + ZKFS.pathForHash(page.authKey().authenticate(file.getPageTag(i)));
			Stat stat = file.fs.archive.storage.stat(path);
			assertEquals(0, stat.getAtime());
			assertEquals(0, stat.getMtime());
		}
	}

	// TODO: test squashing of page merkel chunk timestamps
}
