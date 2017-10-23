package com.acrescrypto.zksync.fs;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;

import com.acrescrypto.zksync.exceptions.*;
import com.acrescrypto.zksync.fs.File;

public class FileTestBase {
	protected FS scratch;

	@Test
	public void testOpenForReadingAllowsReading() throws IOException {
		scratch.write("read-allows-read", "text".getBytes());
		File file = scratch.open("read-allows-read", File.O_RDONLY);
		assertTrue(Arrays.equals("text".getBytes(), file.read()));
		file.close();
	}

	@Test(expected=EACCESException.class)
	public void testOpenForReadingDoesntAllowWriting() throws IOException {
		scratch.write("read-doesnt-allow-write", "text".getBytes());
		File file = scratch.open("read-doesnt-allow-write", File.O_RDONLY); // TODO: LocalFile passed this with File.O_WRONLY... check that out.
		file.write("and more".getBytes());
	}

	@Test
	public void testOpenForWritingAllowsWriting() throws IOException {
		File file = scratch.open("write-allows-write", File.O_WRONLY|File.O_CREAT);
		file.write("i have data".getBytes());
		file.close();
		assertTrue(Arrays.equals("i have data".getBytes(), scratch.read("write-allows-write")));
	}

	@Test(expected=EACCESException.class)
	public void testOpenForWritingDoesntAllowReading() throws IOException {
		scratch.write("write-doesnt-allow-read", "text".getBytes());
		File file = scratch.open("write-doesnt-allow-read", File.O_WRONLY);
		file.read();
		file.close();
	}

	@Test
	public void testOpenForReadWriteAllowsReadWrite() throws IOException {
		scratch.write("readwrite-allows-readwrite", "read".getBytes());
		File file = scratch.open("readwrite-allows-readwrite", File.O_RDWR);
		assertTrue(Arrays.equals(file.read(), "read".getBytes()));
		file.write("write".getBytes());
		file.close();
		assertTrue(Arrays.equals(scratch.read("readwrite-allows-readwrite"), "readwrite".getBytes()));
	}
	
	@Test
	public void testWritingDoesntTruncateFileWithoutOTRUNC() throws IOException {
		scratch.write("write-without-oappend-truncates", "0123456789".getBytes());
		assertEquals(10, scratch.stat("write-without-oappend-truncates").getSize());
		File file = scratch.open("write-without-oappend-truncates", File.O_WRONLY);
		file.close();
		assertEquals(10, scratch.stat("write-without-oappend-truncates").getSize());
	}

	@Test
	public void testWritingTruncatesFileWithOTRUNC() throws IOException {
		scratch.write("write-without-oappend-truncates", "0123456789".getBytes());
		assertEquals(10, scratch.stat("write-without-oappend-truncates").getSize());
		File file = scratch.open("write-without-oappend-truncates", File.O_WRONLY|File.O_TRUNC);
		file.close();
		assertEquals(0, scratch.stat("write-without-oappend-truncates").getSize());
	}
	
	@Test
	public void testOpeningWithOAPPENDJumpsToEndOfFile() throws IOException {
		scratch.write("write-with-oappend-jumps-to-end-of-file", "0123456789".getBytes());
		assertEquals(10, scratch.stat("write-with-oappend-jumps-to-end-of-file").getSize());
		File file = scratch.open("write-with-oappend-jumps-to-end-of-file", File.O_WRONLY|File.O_APPEND);
		assertEquals(10, file.pos());
		file.close();
	}
	
	@Test
	public void testOpeningWithoutOAPPENDStartsAtZero() throws IOException {
		scratch.write("write-without-oappend-jumps-to-end-of-file", "0123456789".getBytes());
		assertEquals(10, scratch.stat("write-without-oappend-jumps-to-end-of-file").getSize());
		File file = scratch.open("write-without-oappend-jumps-to-end-of-file", File.O_WRONLY);
		assertEquals(0, file.pos());
		file.close();
	}

	@Test(expected=ENOENTException.class)
	public void testOCREATNeededToCreateFile() throws IOException {
		assertFalse(scratch.exists("shouldnt-exist"));
		scratch.open("shouldnt-exist", File.O_WRONLY);
	}

	@Test
	public void testOCREATCreatesFile() throws IOException {
		assertFalse(scratch.exists("ocreat-creates-file"));
		scratch.open("ocreat-creates-file", File.O_WRONLY|File.O_CREAT).close();
		assertTrue(scratch.exists("ocreat-creates-file"));
	}

	@Test
	public void testOpenFollowsSymlinks() throws IOException {
		byte[] text = "some content".getBytes();
		scratch.write("open-follows-symlinks-target", text);
		scratch.symlink("open-follows-symlinks-target", "open-follows-symlinks-link");
		File file = scratch.open("open-follows-symlinks-link", File.O_RDONLY);
		assertTrue(Arrays.equals(text, file.read()));
		file.close();
	}

	@Test(expected=EMLINKException.class)
	public void testONOFOLLOWDoesNotFollowSymlinks() throws IOException {
		// TODO: it is not good that ZKFS has different semantics here. LocalFile throws EMLINK, which is kinda Linux-like
		byte[] text = "some content".getBytes();
		scratch.write("open-NOFOLLOW-doesnt-follows-symlinks-target", text);
		scratch.symlink("open-NOFOLLOW-doesnt-follows-symlinks-target", "open-NOFOLLOW-doesnt-follows-symlinks-link");
		File file = scratch.open("open-NOFOLLOW-doesnt-follows-symlinks-link", File.O_RDONLY|File.O_NOFOLLOW);
		assertTrue(file.getStat().isSymlink());
		file.close();
	}

	@Test(expected=EISDIRException.class)
	public void testOpenFailsForDirectories() throws IOException {
		scratch.mkdir("open-on-directory-fails");
		scratch.open("open-on-directory-fails", File.O_RDONLY).close();
	}

	@Test
	public void testTruncateShortensFiles() throws IOException {
		scratch.write("truncate-shortens-files", "123456789".getBytes());
		File file = scratch.open("truncate-shortens-files", File.O_RDWR);
		assertEquals(9, file.getStat().getSize());
		file.truncate(5);
		assertEquals(5, file.getStat().getSize());
		assertTrue(Arrays.equals("12345".getBytes(), file.read()));
	}

	@Test
	public void testTruncateLengthensWithNullPadding() throws IOException {
		byte[] text = "123456789".getBytes();
		scratch.write("truncate-lengthens-files", text);
		File file = scratch.open("truncate-lengthens-files", File.O_RDWR);
		assertEquals(text.length, file.getStat().getSize());
		file.truncate(2*text.length);
		assertEquals(2*text.length, file.getStat().getSize());
		
		ByteBuffer buf = ByteBuffer.allocate(2*text.length);
		buf.put(text);
		assertTrue(Arrays.equals(buf.array(), file.read()));
	}

	@Test(expected=EACCESException.class)
	public void testTruncateRequiresWriteAccess() throws IOException {
		scratch.write("truncate-requires-write-access", "some text".getBytes());
		File file = scratch.open("truncate-requires-write-access", File.O_RDONLY);
		try { file.truncate(0); } finally { file.close(); }
	}

	@Test
	public void testRead() throws IOException {
		byte[] text = "some text".getBytes();
		scratch.write("read", text);
		File file = scratch.open("read", File.O_RDONLY);
		assertTrue(Arrays.equals(text, file.read()));
	}

	@Test
	public void testReadWithArguments() throws IOException {
		byte[] text = "some text".getBytes();
		scratch.write("read", text);
		File file = scratch.open("read", File.O_RDONLY);
		byte[] buf = new byte[2*text.length];
		ByteBuffer ref = ByteBuffer.allocate(2*text.length);
		ref.position(4);
		ref.put(text, 0, text.length-1);
		file.read(buf, 4, text.length-1);
		assertTrue(Arrays.equals(ref.array(), buf));
	}

	@Test
	public void testWrite() throws IOException {
		byte[] text = "do you wanna have a bad time?".getBytes();
		File file = scratch.open("write", File.O_CREAT|File.O_WRONLY);
		file.write(text);
		file.close();
		assertTrue(Arrays.equals(scratch.read("write"), text));
	}

	@Test
	public void testWriteAtLocation() throws IOException {
		byte[] text = "because if you take one more step, you're really not gonna like what happens next.".getBytes();
		File file = scratch.open("write-at-location", File.O_CREAT|File.O_WRONLY);
		file.truncate(256);
		file.seek(16, File.SEEK_SET);
		file.write(text);
		file.close();
		
		ByteBuffer ref = ByteBuffer.allocate(256);
		ref.position(16);
		ref.put(text);
		
		assertTrue(Arrays.equals(ref.array(), scratch.read("write-at-location")));
	}

	@Test
	public void testSeekSet() throws IOException {
		File file = scratch.open("seek-set", File.O_CREAT|File.O_WRONLY);
		file.truncate(256);
		file.seek(10, File.SEEK_SET);
		assertEquals(10, file.pos());
	}

	@Test
	public void testSeekCur() throws IOException {
		File file = scratch.open("seek-set", File.O_CREAT|File.O_WRONLY);
		file.truncate(256);
		file.seek(10, File.SEEK_CUR);
		assertEquals(10, file.pos());
	}

	@Test
	public void testSeekEnd() throws IOException {
		File file = scratch.open("seek-set", File.O_CREAT|File.O_WRONLY);
		file.truncate(256);
		file.seek(0, File.SEEK_END);
		assertEquals(256, file.pos());
	}

	@Test
	public void testCopy() {
		// TODO
	}
}
