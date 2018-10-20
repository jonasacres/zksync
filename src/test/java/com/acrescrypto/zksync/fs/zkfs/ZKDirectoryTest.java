package com.acrescrypto.zksync.fs.zkfs;

import static org.junit.Assert.assertTrue;

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
import com.acrescrypto.zksync.exceptions.InvalidArchiveException;
import com.acrescrypto.zksync.fs.DirectoryTestBase;
import com.acrescrypto.zksync.utility.Util;

public class ZKDirectoryTest extends DirectoryTestBase {
	ZKFS zkscratch;
	ZKMaster master;
	
	@Before
	public void beforeEach() throws IOException {
		ZKFSTest.cheapenArgon2Costs();
		master = ZKMaster.openBlankTestVolume();
		scratch = zkscratch = master.createArchive(ZKArchive.DEFAULT_PAGE_SIZE, "").openBlank();
	}
	
	@After
	public void afterEach() throws IOException {
		ZKFSTest.restoreArgon2Costs();
		master.close();
		zkscratch.close();
		zkscratch.archive.close();
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
}
