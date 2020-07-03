package com.acrescrypto.zksync.fs;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class FSPathTest {
	@Test
	public void testRecognizesWindowsAbsolute() {
		String pp = new FSPath("C:\\absolute\\path").toPosix();
		assertEquals("/C/absolute/path", pp);
	}
	
	@Test
	public void testStandardizesDriveLetterCaseAsCapital() {
		String pp = new FSPath("d:\\absolute\\path").toPosix();
		assertEquals("/D/absolute/path", pp);
	}

	@Test
	public void testRecognizesBastardWindowsAbsolute() {
		String pp = new FSPath("C:/absolute/path").toPosix();
		assertEquals("/C/absolute/path", pp);
	}

	@Test
	public void testRecognizesWindowsRelative() {
		String pp = new FSPath("relative\\path").toPosix();
		assertEquals("relative/path", pp);
	}
	
	@Test
	public void testRecognizesBastardWindowsRelative() {
		String pp = new FSPath("C:/absolute/path").toPosix();
		assertEquals("/C/absolute/path", pp);
	}
	
	@Test
	public void testRecognizesWindowsAbsoluteWithTrailingDelimiter() {
		String pp = new FSPath("C:\\absolute\\path\\").toPosix();
		assertEquals("/C/absolute/path/", pp);
	}
	
	@Test
	public void testRecognizesBastardWindowsAbsoluteWithTrailingDelimiter() {
		String pp = new FSPath("C:/absolute/path/").toPosix();
		assertEquals("/C/absolute/path/", pp);
	}
	
	@Test
	public void testImpliedDriveLetterAddedForWindows() {
		String pp = new FSPath("\\absolute\\path\\").toWindows();
		assertEquals("C:\\absolute\\path\\", pp);
	}
	
	@Test
	public void testImpliedDriveLetterNotAddedForPosix() {
		String pp = new FSPath("\\absolute\\path\\").toPosix();
		assertEquals("/absolute/path/", pp);
	}
	
	@Test
	public void testDefaultDriveAllowsChangingImpliedDriveRoot() {
		String pp = new FSPath("\\absolute\\path\\").defaultDrive("Z").toWindows();
		assertEquals("Z:\\absolute\\path\\", pp);
	}
	
	@Test
	public void testDefaultDriveDoesNotAffectExplicitDriveRoots() {
		String pp = new FSPath("C:\\absolute\\path\\").defaultDrive("Z").toPosix();
		assertEquals("/C/absolute/path/", pp);
	}
	
	@Test
	public void testRecognizesWindowsRelativeWithTrailingDelimiter() {
		String pp = new FSPath("relative\\path\\").toPosix();
		assertEquals("relative/path/", pp);
	}
	
	@Test
	public void testRecognizesPosixAbsolute() {
		String pp = new FSPath("/absolute/path").toPosix();
		assertEquals("/absolute/path", pp);
	}
	
	@Test
	public void testRecognizesPosixRelative() {
		String pp = new FSPath("relative/path").toPosix();
		assertEquals("relative/path", pp);
	}
	
	@Test
	public void testRecognizesPosixAbsoluteWithTrailingDelimiter() {
		String pp = new FSPath("/absolute/path/").toPosix();
		assertEquals("/absolute/path/", pp);
	}
	
	@Test
	public void testRecognizesPosixRelativeWithTrailingDelimiter() {
		String pp = new FSPath("relative/path/").toPosix();
		assertEquals("relative/path/", pp);
	}
	
	@Test
	public void testToNativeGeneratesWindowsPathFromWindowsOnWindows() {
		String pp = new FSPath("C:\\absolute\\path").platform("windows").toNative();
		assertEquals("C:\\absolute\\path", pp);
	}

	@Test
	public void testToNativeGeneratesWindowsPathFromPosixOnWindows() {
		String pp = new FSPath("/absolute/path").platform("windows").toNative();
		assertEquals("C:\\absolute\\path", pp);
	}
	
	@Test
	public void testToNativeGeneratesPosixPathFromWindowsOnPosix() {
		String pp = new FSPath("C:\\absolute\\path").platform("posix").toNative();
		assertEquals("/C/absolute/path", pp);
	}
	
	@Test
	public void testToNativeGeneratesPosixPathFromPosixOnPosix() {
		String pp = new FSPath("/absolute/path").platform("posix").toNative();
		assertEquals("/absolute/path", pp);
	}
	
	@Test
	public void testSourcePlatformGeneratesWindowsPathWhenOriginalWasWindows() {
		String pp = new FSPath("C:\\absolute\\path").toSourcePlatform();
		assertEquals("C:\\absolute\\path", pp);
	}
	
	@Test
	public void testSourcePlatformGeneratesPosixPathWhenOriginalWasPosix() {
		String pp = new FSPath("/absolute/path").toSourcePlatform();
		assertEquals("/absolute/path", pp);
	}
	
	@Test
	public void testToPlatformWithWindowsGenreatesWindowsPath() {
		String pp = new FSPath("/absolute/path").toPlatform("windows");
		assertEquals("C:\\absolute\\path", pp);
	}
	
	@Test
	public void testToPlatformWithPosixGenreatesPosixPath() {
		String pp = new FSPath("C:\\absolute\\path").toPlatform("posix");
		assertEquals("/C/absolute/path", pp);
	}
	
	@Test
	public void testJoinConcatenatesTwoPaths() {
		String pp = new FSPath("/path/to").join("the/thing").toPosix();
		assertEquals("/path/to/the/thing", pp);
	}
	
	@Test
	public void testJoinConcatenatesTwoAbsolutePosixPaths() {
		String pp = new FSPath("/path/to").join("/the/thing").toPosix();
		assertEquals("/path/to/the/thing", pp);
	}
	
	@Test
	public void testJoinConcatenatesPosixToWindows() {
		String pp = new FSPath("/path/to").join("the\\thing").toPosix();
		assertEquals("/path/to/the/thing", pp);
	}
	
	@Test
	public void testJoinConcatenatesWindowsToPosix() {
		String pp = new FSPath("C:\\path\\to").join("the/thing").toPosix();
		assertEquals("/C/path/to/the/thing", pp);
	}
	
	@Test
	public void testJoinConcatenatesSemiabsoluteWindowsToPosix() {
		String pp = new FSPath("\\path\\to\\").join("the/thing").toPosix();
		assertEquals("/path/to/the/thing", pp);
	}
	
	@Test
	public void testJoinConcatenatesPosixToSemiabsoluteWindows() {
		String pp = new FSPath("/path/to").join("\\the\\thing").toPosix();
		assertEquals("/path/to/the/thing", pp);
	}
	
	@Test
	public void testStaticJoinConcatenatesAllPaths() {
		String pp = FSPath.join("/path/to", "the/", "thing\\in", "/many//parts/").toPosix();
		assertEquals("/path/to/the/thing/in/many/parts/", pp);
	}
	
	@Test
	public void testNormalizeDoesNotModifyNonRedundantPaths() {
		String pp = new FSPath("/not/redundant").normalize().toPosix();
		assertEquals("/not/redundant", pp);
	}
	
	@Test
	public void testNormalizeRemovesDotAtEnd() {
		String pp = new FSPath("/no/dot/please/.").normalize().toPosix();
		assertEquals("/no/dot/please", pp);
	}
	
	@Test
	public void testNormalizeRemovesDotInMiddle() {
		String pp = new FSPath("/no/dot/please/./thanks").normalize().toPosix();
		assertEquals("/no/dot/please/thanks", pp);
	}
	
	@Test
	public void testNormalizeRemovesDotAtStart() {
		String pp = new FSPath("./no/dot/please/thanks").normalize().toPosix();
		assertEquals("no/dot/please/thanks", pp);
	}
	
	@Test
	public void testNormalizePreservesDotDotAtStart() {
		String pp = new FSPath("../1/2/3").normalize().toPosix();
		assertEquals("../1/2/3", pp);
	}
	
	@Test
	public void testNormalizeRemovesDotDotAndParentInMiddle() {
		String pp = new FSPath("1/2/../3").normalize().toPosix();
		assertEquals("1/3", pp);
	}
	
	@Test
	public void testNormalizeRemovesDotDotAndParentAtEnd() {
		String pp = new FSPath("1/2/3/..").normalize().toPosix();
		assertEquals("1/2", pp);
	}
	
	@Test
	public void testNormalizePreservesOriginalPath() {
		FSPath path = new FSPath("1/2/3/..").normalize();
		assertEquals("1/2/3/..", path.original());
	}
	
	@Test
	public void testNormalizePreservesOriginalSourcePlatform() {
		FSPath path0 = new FSPath("1/2/3/..")   .normalize();
		FSPath path1 = new FSPath("1\\2\\3\\..").normalize();
		assertEquals("posix",   path0.sourcePlatform());
		assertEquals("windows", path1.sourcePlatform());
	}
	
	@Test
	public void testNormalizePreservesDriveLetter() {
		FSPath path = new FSPath("D:\\test").normalize();
		assertEquals("D", path.drive());
	}
	
	@Test
	public void testNormalizePreservesTrailingDelimiter() {
		String pp = new FSPath("D:\\test\\").normalize().toPosix();
		assertEquals("/D/test/", pp);
	}
}
