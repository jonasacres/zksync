package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;
import java.util.Arrays;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FileTestBase;
import com.acrescrypto.zksync.fs.Stat;

public class ZKFileTest extends FileTestBase {
	ZKFS zkscratch;
	ZKMaster master;
	
	@BeforeClass
	public static void beforeClass() {
		TestUtils.startDebugMode();
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@Before
	public void beforeEach() throws IOException {
		master = ZKMaster.openBlankTestVolume();
		scratch = zkscratch = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
	}
	
	@After
	public void afterEach() throws IOException {
		zkscratch.archive.close();
		zkscratch.close();
		master.close();
	}
	
	@AfterClass
	public static void afterClass() throws IOException {
		TestUtils.stopDebugMode();
		TestUtils.assertTidy();
	}

	@Test
	public void testImmedateWrite() throws IOException {
		byte[] contents = new byte[zkscratch.archive.crypto.hashLength()-1];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		ZKFile file = zkscratch.open("immediate-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();
		assertEquals(RefTag.REF_TYPE_IMMEDIATE, file.inode.getRefTag().refType);
		
		file = zkscratch.open("immediate-write-test", ZKFile.O_RDONLY);
		assertEquals(RefTag.REF_TYPE_IMMEDIATE, file.inode.getRefTag().refType);
		assertTrue(Arrays.equals(file.read(), contents));
		file.close();
	}
	
	@Test
	public void testSinglePageWrite() throws IOException {
		byte[] contents = new byte[zkscratch.archive.config.pageSize];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		ZKFile file = zkscratch.open("singlepage-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();
		
		file = zkscratch.open("singlepage-write-test", ZKFile.O_RDONLY);
		assertTrue(Arrays.equals(file.read(), contents));
		assertEquals(RefTag.REF_TYPE_INDIRECT, file.inode.getRefTag().refType);
		file.close();
	}
	
	@Test
	public void testMultipageWrite() throws IOException {
		byte[] contents = new byte[5*zkscratch.archive.config.pageSize];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		
		ZKFile file = zkscratch.open("multipage-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();
		
		file = zkscratch.open("multipage-write-test", ZKFile.O_RDONLY);
		assertTrue(Arrays.equals(file.read(), contents));
		assertEquals(RefTag.REF_TYPE_2INDIRECT, file.inode.getRefTag().refType);
		file.close();
	}
	
	@Test
	public void testPageTimestampSquashing() throws IOException {
		byte[] contents = new byte[5*zkscratch.archive.config.pageSize];
		for(int i = 0; i < contents.length; i++) contents[i] = (byte) (i & 0xff);
		
		ZKFile file = zkscratch.open("multipage-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		file.write(contents);
		file.close();

		for(int i = 0; i < Math.ceil(((double) file.getStat().getSize())/file.zkfs.archive.config.pageSize); i++) {
			Page page = new Page(file, i);
			String path = Page.pathForTag(page.authKey().authenticate(file.getPageTag(i)));
			Stat stat = file.zkfs.archive.storage.stat(path);
			assertEquals(0, stat.getAtime());
			assertEquals(0, stat.getMtime());
		}
	}
	
	@Test
	public void testRandomWrites() throws IOException {
		int pageSize = zkscratch.archive.config.pageSize;
		ZKFile file = zkscratch.open("random-write-test", ZKFile.O_CREAT|ZKFile.O_RDWR);
		
		file.truncate(5*pageSize);
		file.seek(2*pageSize, ZKFile.SEEK_SET);
		file.write("bar".getBytes());
		file.seek(1*pageSize, ZKFile.SEEK_SET);
		file.write("foo".getBytes());
		file.seek(4*pageSize-1, ZKFile.SEEK_SET);
		file.write("split".getBytes());
		
		file.seek(1*pageSize, ZKFile.SEEK_SET);
		assertEquals("foo", new String(file.read(3)));
		file.seek(2*pageSize, ZKFile.SEEK_SET);
		assertEquals("bar", new String(file.read(3)));
		file.seek(4*pageSize-1, ZKFile.SEEK_SET);
		assertEquals("split", new String(file.read(5)));
		assertEquals(5*pageSize, file.getSize());
		file.close();
		
		ByteBuffer wholeThing = ByteBuffer.allocate(5*pageSize);
		wholeThing.position(1*pageSize);
		wholeThing.put("foo".getBytes());
		wholeThing.position(2*pageSize);
		wholeThing.put("bar".getBytes());
		wholeThing.position(4*pageSize-1);
		wholeThing.put("split".getBytes());
		assertTrue(Arrays.equals(wholeThing.array(), zkscratch.read("random-write-test")));
	}
	
	@Test
	public void testWriteBlank() throws IOException {
		ZKFile file = zkscratch.open("blank", ZKFile.O_CREAT|ZKFile.O_WRONLY);
		file.write(new byte[0]);
		file.flush();
		file.close();
		assertEquals(0, zkscratch.read("blank").length);
	}
	
	@Test
	public void testGraduateSizes() throws IOException {
		// what happens when we grow past the threshold to need multiple chunks in our pagetree?
		String path = "graduate-sizes";
		ZKFile file = zkscratch.open(path, ZKFile.O_CREAT|ZKFile.O_WRONLY|ZKFile.O_TRUNC);
		int pagesPerChunk = zkscratch.archive.config.pageSize / master.crypto.hashLength();
		
		byte[] onePage = new byte[zkscratch.archive.config.pageSize];
		for(int i = 0; i < pagesPerChunk+1; i++) {
			file.write(onePage);
		}
		file.close();
	}
	
	@Test
	public void testRefTagChangeFromImmediateToIndirect() throws IOException {
		String path = "foo";
		int hashLen = master.getCrypto().hashLength();
		int pageSize = zkscratch.getArchive().getConfig().getPageSize();
		byte[] data = master.getCrypto().defaultPrng().getBytes(pageSize);
		
		try(ZKFile file = zkscratch.open(path, ZKFile.O_CREAT|ZKFile.O_WRONLY|ZKFile.O_TRUNC)) {
			file.write(data, 0, hashLen-1);
			file.flush();
			assertEquals(hashLen-1, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_IMMEDIATE, file.getInode().getRefTag().getRefType());
			
			file.write(data, hashLen-1, data.length - hashLen + 1);
			file.flush();
			assertEquals(data.length, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_INDIRECT, file.getInode().getRefTag().getRefType());
		}
	}

	@Test
	public void testRefTagChangeFromImmediateTo2Indirect() throws IOException {
		String path = "foo";
		int hashLen = master.getCrypto().hashLength();
		int pageSize = zkscratch.getArchive().getConfig().getPageSize();
		byte[] data = master.getCrypto().defaultPrng().getBytes(2 * pageSize);
		
		try(ZKFile file = zkscratch.open(path, ZKFile.O_CREAT|ZKFile.O_WRONLY|ZKFile.O_TRUNC)) {
			file.write(data, 0, hashLen-1);
			file.flush();
			assertEquals(hashLen-1, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_IMMEDIATE, file.getInode().getRefTag().getRefType());
			
			file.write(data, hashLen-1, data.length - hashLen + 1);
			file.flush();
			assertEquals(data.length, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_2INDIRECT, file.getInode().getRefTag().getRefType());
		}
	}

	@Test
	public void testRefTagChangeFromIndirectToImmediate() throws IOException {
		String path = "foo";
		int hashLen = master.getCrypto().hashLength();
		int pageSize = zkscratch.getArchive().getConfig().getPageSize();
		byte[] data = master.getCrypto().defaultPrng().getBytes(pageSize - 1);
		
		try(ZKFile file = zkscratch.open(path, ZKFile.O_CREAT|ZKFile.O_WRONLY|ZKFile.O_TRUNC)) {
			file.write(data);
			file.flush();
			assertEquals(data.length, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_INDIRECT, file.getInode().getRefTag().getRefType());
			
			/* we could do a truncate(hashLen-1) here, but doing it this way triggered the bug that caused
			 * this set of tests to be written...
			 */
			file.truncate(0);
			file.write(data, 0, hashLen-1);
			file.flush();
			assertEquals(hashLen-1, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_IMMEDIATE, file.getInode().getRefTag().getRefType());
		}
	}
	
	@Test
	public void testRefTagChangeFromIndirectTo2Indirect() throws IOException {
		String path = "foo";
		int pageSize = zkscratch.getArchive().getConfig().getPageSize();
		byte[] data = master.getCrypto().defaultPrng().getBytes(2 * pageSize);
		
		try(ZKFile file = zkscratch.open(path, ZKFile.O_CREAT|ZKFile.O_WRONLY|ZKFile.O_TRUNC)) {
			file.write(data, 0, pageSize-1);
			file.flush();
			assertEquals(pageSize-1, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_INDIRECT, file.getInode().getRefTag().getRefType());
			
			file.write(data, pageSize-1, data.length - pageSize + 1);
			file.flush();
			assertEquals(data.length, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_2INDIRECT, file.getInode().getRefTag().getRefType());
		}
	}

	@Test
	public void testRefTagChangeFrom2IndirectToImmediate() throws IOException {
		String path = "foo";
		int hashLen = master.getCrypto().hashLength();
		int pageSize = zkscratch.getArchive().getConfig().getPageSize();
		byte[] data = master.getCrypto().defaultPrng().getBytes(2*pageSize - 1);
		
		try(ZKFile file = zkscratch.open(path, ZKFile.O_CREAT|ZKFile.O_WRONLY|ZKFile.O_TRUNC)) {
			file.write(data);
			file.flush();
			assertEquals(data.length, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_2INDIRECT, file.getInode().getRefTag().getRefType());
			
			file.truncate(0);
			file.write(data, 0, hashLen-1);
			file.flush();
			assertEquals(hashLen-1, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_IMMEDIATE, file.getInode().getRefTag().getRefType());
		}
	}
	
	@Test
	public void testRefTagChangeFrom2IndirectToIndirect() throws IOException {
		String path = "foo";
		int pageSize = zkscratch.getArchive().getConfig().getPageSize();
		byte[] data = master.getCrypto().defaultPrng().getBytes(2*pageSize - 1);
		
		try(ZKFile file = zkscratch.open(path, ZKFile.O_CREAT|ZKFile.O_WRONLY|ZKFile.O_TRUNC)) {
			file.write(data);
			file.flush();
			assertEquals(data.length, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_2INDIRECT, file.getInode().getRefTag().getRefType());
			
			// try this one as a straight truncation just to mix things up
			file.truncate(pageSize-1);
			file.flush();
			assertEquals(pageSize-1, zkscratch.stat(path).getSize());
			assertEquals(RefTag.REF_TYPE_INDIRECT, file.getInode().getRefTag().getRefType());
		}
	}
}
