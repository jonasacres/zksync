package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.exceptions.EINVALException;
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.fs.DirectoryTestBase;

public class ZKDirectoryTest extends DirectoryTestBase {
	ZKFS zkscratch;
	ZKMaster master;
	
	@Before
	public void beforeEach() throws IOException {
		TestUtils.startDebugMode();
		master = ZKMaster.openBlankTestVolume();
		scratch = zkscratch = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
	}
	
	@After
	public void afterEach() throws IOException {
		master.close();
		zkscratch.close();
		zkscratch.archive.close();
		TestUtils.stopDebugMode();
	}

	@BeforeClass
	public static void beforeClass() {
		Security.addProvider(new BouncyCastleProvider());
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test(expected=InvalidArchiveException.class)
	public void testDirectoriesWithInvalidInodeIdsCauseException() throws IOException {
		scratch.mkdir("evil");
		scratch.mkdir("evil/thisisasubdirectory");
		zkscratch.commit();
		
		ZKDirectory evilDirectory = new ZKDirectory(zkscratch, "evil") {
			@Override
			protected byte[] serialize() throws IOException {
				byte[] serialization = super.serialize();
				ByteBuffer.wrap(serialization).putLong(Long.MIN_VALUE);
				return serialization;
			}
		};
		
		evilDirectory.dirty = true;
		evilDirectory.commit();
		evilDirectory.close();
		zkscratch.commit();
		
		assertTrue(scratch.exists("evil"));
		assertTrue(scratch.stat("evil").isDirectory());
		new ZKDirectory(zkscratch, "evil").close();
	}

	@Test(expected=InvalidArchiveException.class)
	public void testDirectoriesWithNegativePathLengthsCauseException() throws IOException {
		scratch.mkdir("evil");
		scratch.mkdir("evil/thisisasubdirectory");
		zkscratch.commit();
		
		ZKDirectory evilDirectory = new ZKDirectory(zkscratch, "evil") {
			@Override
			protected byte[] serialize() throws IOException {
				byte[] serialization = super.serialize();
				ByteBuffer buf = ByteBuffer.wrap(serialization);
				buf.position(8);
				buf.putLong(Long.MIN_VALUE);
				return serialization;
			}
		};
		
		evilDirectory.dirty = true;
		evilDirectory.commit();
		evilDirectory.close();
		zkscratch.commit();
		
		assertTrue(scratch.exists("evil"));
		assertTrue(scratch.stat("evil").isDirectory());
		new ZKDirectory(zkscratch, "evil").close();
	}
	
	@Test(expected=InvalidArchiveException.class)
	public void testDirectoriesWithTruncatedPathsCauseException() throws IOException {
		scratch.mkdir("evil");
		scratch.mkdir("evil/thisisasubdirectory");
		zkscratch.commit();
		
		ZKDirectory evilDirectory = new ZKDirectory(zkscratch, "evil") {
			@Override
			protected byte[] serialize() throws IOException {
				byte[] serialization = super.serialize();
				ByteBuffer buf = ByteBuffer.wrap(serialization);
				buf.position(8);
				buf.putLong(100); // tell reader to expect 100 character path, much longer than reality
				return serialization;
			}
		};
		
		evilDirectory.dirty = true;
		evilDirectory.commit();
		evilDirectory.close();
		zkscratch.commit();
		
		assertTrue(scratch.exists("evil"));
		assertTrue(scratch.stat("evil").isDirectory());
		new ZKDirectory(zkscratch, "evil").close();
	}

	@Test(expected=InvalidArchiveException.class)
	public void testDirectoriesWithIllegalPathLengthsCauseException() throws IOException {
		scratch.mkdir("evil");
		for(int i = 0; i < 100; i++) {
			scratch.mkdir("evil/thisisasubdirectory" + i);
		}
		zkscratch.commit();
		
		ZKDirectory evilDirectory = new ZKDirectory(zkscratch, "evil") {
			@Override
			protected byte[] serialize() throws IOException {
				byte[] serialization = super.serialize();
				ByteBuffer buf = ByteBuffer.wrap(serialization);
				buf.position(8);
				buf.putLong(ZKDirectory.MAX_NAME_LEN+1);
				return serialization;
			}
		};
		
		evilDirectory.dirty = true;
		evilDirectory.commit();
		evilDirectory.close();
		zkscratch.commit();
		
		assertTrue(scratch.exists("evil"));
		assertTrue(scratch.stat("evil").isDirectory());
		new ZKDirectory(zkscratch, "evil").close();
	}
	
	@Test
	public void testLinkThrowsExceptionIfInvalidCharactersUsed() throws IOException {
		String illegalNames[] = new String[] { "a/file", "c:\\test", new String(new byte[] { 0x74, 0x65, 0x73, 0x74, 0x00 }) };
		scratch.mkdir("test");
		scratch.write("testfile", "contents".getBytes());
		ZKDirectory dir = new ZKDirectory(zkscratch, "test");
		for(String name : illegalNames) {
			try {
				dir.link("testfile", name);
				fail("Expected EINVALException for " + name);
			} catch(EINVALException exc) {
				assertFalse(dir.contains(name));
			}
		}
		dir.close();
	}
	
	@Test
	public void testDeserializationIgnoresInvalidPaths() throws IOException {
		String[] names = {
				"c:\\evil",
				"a/nefarious",
				new String(new byte[] { 0x62, 0x61, 0x64, 0x00 })
		};

		scratch.write("testfile", "contents".getBytes());

		int n = 0;
		for(String name : names) {
			n++;
			zkscratch.mkdir("evil" + n);
			zkscratch.directoriesByPath.removeAll();
			
			ZKDirectory evilDirectory = new ZKDirectory(zkscratch, "evil" + n) {
				@Override public boolean isValidName(String path) { return true; }
			};

			evilDirectory.link("testfile", name);
			evilDirectory.link("testfile", "valid");
			evilDirectory.commit();
			evilDirectory.close();
			
			zkscratch.directoriesByPath.removeAll();
			ZKDirectory innocentDirectory = zkscratch.opendir("evil" + n);
			assertEquals(1, innocentDirectory.list().length);
			assertEquals("valid", innocentDirectory.list()[0]);
		}
	}
	
	@Test
	public void testHandlesEmoji() throws IOException {
		"".length();
		String name = "i'm sure glad we have these 🖕 to deal with now";
		zkscratch.mkdir("dir");
		zkscratch.write("normalfile", "blah".getBytes());
		
		ZKDirectory dir = zkscratch.opendir("dir");
		dir.link("normalfile", name);
		dir.close();
		
		try(ZKFS fs = zkscratch.commit().getFS()) {
			dir = fs.opendir("dir");
			String[] list = dir.list();
			dir.close();
			
			assertEquals(name, list[0]);
		}
	}
	
	@Test
	public void testMatchesConfigPermissions() throws IOException {
		zkscratch.mkdir("dir");
		assertEquals(zkscratch.archive.master.getGlobalConfig().getInt("fs.default.directoryMode"), zkscratch.stat("dir").getMode());
	}
}
