package com.acrescrypto.zksync.fs.zkfs;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;

import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.FreeList.FreeListExhaustedException;
import com.acrescrypto.zksync.utility.Util;

public class IntegrityChecker {
	protected ZKFS fs;
	protected HashMap<Long,Integer> linkCounts = new HashMap<>();
	protected HashMap<Long,Integer> identityCounts = new HashMap<>();
	protected LinkedList<Long> freelistContents = new LinkedList<>();
	
	public static void assertValidFilesystem(RevisionTag revTag) throws IOException {
		try(ZKFS fs = revTag.getFS()) {
			IntegrityChecker checker = new IntegrityChecker(fs);
			Collection<IntegrityIssue> issues = checker.findIssues();
			if(issues.size() == 0) return;
			
			Util.debugLog(String.format("IntegrityChecker %s:\n%s\n%s\n%s\nDead.",
					fs.getArchive().getMaster().getName(),
					checker.dumpIssues(issues),
					fs.dump(),
					fs.inodeTable.dumpInodes()));
			System.exit(1);
		} catch(Exception exc) {
			exc.printStackTrace();
			System.exit(1);
		}
	}
	
	public class IntegrityIssue {
		String description;
		public String getDescription() { return description; }
		
		public IntegrityIssue(String description) {
			this.description = description;
		}
	}
	
	public class IntegrityIssueInode extends IntegrityIssue {
		Inode inode;
		long expectedId;
		
		public IntegrityIssueInode(Inode inode, long expectedId, String description) {
			super(description);
			this.inode = inode;
			this.expectedId = expectedId;
		}
		
		public Inode getInode() { return inode; }
		public String getDescription() { return description; }
		public String toString() { 
			return String.format("inodeId %d (%d) identity %016x: %s",
				inode.getStat().getInodeId(),
				expectedId,
				inode.getIdentity(),
				description);
		}
	}
	
	public class IntegrityIssuePath extends IntegrityIssue {
		String path;
		
		public IntegrityIssuePath(String path, String description) {
			super(description);
			this.path = path;
		}
		
		public String toString() { 
			return String.format("Path %s: %s",
				path,
				description);
		}
	}
	
	public IntegrityChecker(ZKFS fs) {
		this.fs = fs;
	}
	
	public Collection<IntegrityIssue> findIssues() throws IOException {
		LinkedList<IntegrityIssue> issues = new LinkedList<>();
		
		scan();
		validateReservedInodes(issues);
		
		for(long inodeId = InodeTable.USER_INODE_ID_START; inodeId < fs.getInodeTable().nextInodeId(); inodeId++) {
			Inode inode = fs.getInodeTable().inodeWithId(inodeId);
			if(inode.isDeleted()) {
				validateDeletedUserInode(issues, inode, inodeId);
			} else {
				validateActiveUserInode(issues, inode, inodeId);
			}
		}
		
		long numSerializedInodes = calculateNumSerializedInodes();
		for(long inodeId = fs.getInodeTable().nextInodeId(); inodeId < numSerializedInodes; inodeId++) {
			Inode inode = fs.getInodeTable().inodeWithId(inodeId);
			validateDeletedUserInode(issues, inode, inodeId);
		}
		
		return issues;
	}
	
	public String dumpIssues(Collection<IntegrityIssue> issues) {
		StringBuilder sb = new StringBuilder();
		sb.append(String.format("ZKFS integrity check revision %s, dirty=%s, %d issues [%s]\n",
				Util.formatRevisionTag(fs.baseRevision),
				fs.dirty ? "true" : "false",
				issues.size(),
				issues.size() == 0 ? "PASS" : "FAIL"));
		
		int issueNum = 0;
		for(IntegrityIssue issue : issues) {
			issueNum++;
			sb.append(String.format("\t%3d  %s\n",
					issueNum,
					issue.toString()));
		}
		
		return sb.toString();
	}
	
	protected long calculateNumSerializedInodes() {
		long size = fs.getInodeTable().getSize();
		long pageSize = fs.getArchive().getConfig().getPageSize();
		int numPages = (int) Math.ceil(((double) size) / pageSize);
		
		if(numPages == 0) return 0; // shouldn't be possible but whatever
		int numInodes = fs.getInodeTable().numInodesForPage(0)
				+ (numPages-1) * fs.getInodeTable().numInodesForPage(1);
		return numInodes;
	}
	
	protected boolean validateReservedInodes(Collection<IntegrityIssue> issues) throws IOException {
		boolean passed = true;
		
		passed &= validateTableInode(issues, fs.getInodeTable().inode);
		passed &= validateRootDirectoryInode(issues, fs.getInodeTable().inodeWithId(InodeTable.INODE_ID_ROOT_DIRECTORY));
		passed &= validateFreelistInode(issues, fs.getInodeTable().inodeWithId(InodeTable.INODE_ID_FREELIST));
		
		return passed;
	}
	
	protected boolean validateTableInode(Collection<IntegrityIssue> issues, Inode inode) {
		return validateReservedInode(issues, inode, InodeTable.INODE_ID_INODE_TABLE);
	}
	
	protected boolean validateRootDirectoryInode(Collection<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		
		passed &= validateReservedInode(issues, inode, InodeTable.INODE_ID_ROOT_DIRECTORY);
		if(!inode.getStat().isDirectory()) {
			passed = false;
			issues.add(new IntegrityIssueInode(inode, InodeTable.INODE_ID_ROOT_DIRECTORY,
					String.format("Expected root directory to have type %02x; has %02x",
					Stat.TYPE_DIRECTORY,
					inode.getStat().getType())));
		}
		
		return passed;
	}
	
	protected boolean validateFreelistInode(Collection<IntegrityIssue> issues, Inode inode) {
		return validateReservedInode(issues, inode, InodeTable.INODE_ID_FREELIST);
	}
	
	protected boolean validateReservedInode(Collection<IntegrityIssue> issues, Inode inode, long expectedId) {
		boolean passed = true;
		
		if(inode.getIdentity() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected identity %016x; got %016x",
							0,
							inode.getIdentity())));
			passed = false;
		}
		
		int expectedLinks = linkCounts.getOrDefault(inode.getStat().getInodeId(), 0);
		if(inode.nlink != expectedLinks) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected nlink %d; got %d",
							expectedLinks,
							inode.nlink)));
			passed = false;
		}
		
		if(inode.getStat().getInodeId() != expectedId) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected inodeId %d; got %d", expectedId, inode.nlink)));
			passed = false;
		}
		
		return passed;
	}
	
	protected boolean validateDeletedUserInode(LinkedList<IntegrityIssue> issues, Inode inode, long expectedId) throws IOException {
		boolean passed = true;
		
		if(expectedId < fs.getInodeTable().nextInodeId()) {
			passed &= validateInFreelist(issues, inode);
		}
		
		if(inode.nlink != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have nlink 0; got %d", inode.nlink)));
			passed = false;
		}
		
		/* accept a formal blank or all zeroes. technically, we should only see all zeroes at the end of
		 * the last page of an inode, for the unused portion of the inode table, but it's not a huge deal. 
		 */
		if(!inode.getRefTag().isBlank() && !Arrays.equals(inode.getRefTag().getBytes(), new byte[inode.getRefTag().getBytes().length])) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have blank reftag; got %s",
							Util.formatRefTag(inode.getRefTag()))));
			passed = false;
		}
		
		RevisionTag blank = RevisionTag.blank(fs.getArchive().getConfig());
		if(!inode.getChangedFrom().equals(blank)) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have blank changedFrom; got %s",
							Util.formatRevisionTag(inode.getChangedFrom()))));
			passed = false;
		}
		
		if(inode.flags != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have flags 0x00; got 0x%02x",
							inode.flags)));
			passed = false;
		}

		if(inode.identity != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have identity 0; got %016x",
							inode.identity)));
			passed = false;
		}
		
		if(inode.getStat().getGid() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have GID 0; had %d",
							inode.getStat().getGid())));
			passed = false;
		}
		
		if(inode.getStat().getUid() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have UID 0; had %d",
							inode.getStat().getUid())));
			passed = false;
		}
		
		if(inode.getStat().getMode() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have mode 0; had %d",
							inode.getStat().getMode())));
			passed = false;
		}
		
		if(inode.getStat().getType() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have type 0x00; had 0x%02x",
							inode.getStat().getType())));
			passed = false;
		}
		
		if(inode.getStat().getDevMajor()!= 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have devMajor 0; had %d",
							inode.getStat().getDevMajor())));
			passed = false;
		}
		
		if(inode.getStat().getDevMinor() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have devMinor 0; had %d",
							inode.getStat().getDevMinor())));
			passed = false;
		}
		
		if(inode.getStat().getGroup().length() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have blank group; had %d bytes '%s'",
							inode.getStat().getGroup() == null ? -1 : inode.getStat().getGroup().length(),
							inode.getStat().getGroup() == null ? "(null)" : inode.getStat().getGroup()
							)));
			passed = false;
		}
		
		if(inode.getStat().getUser().length() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have blank user; had %d bytes '%s'",
							inode.getStat().getUser() == null ? -1 : inode.getStat().getUser().length(),
							inode.getStat().getUser() == null ? "(null)" : inode.getStat().getUser()
							)));
			passed = false;
		}

		if(inode.getStat().getAtime() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have atime 0; had %d",
							inode.getStat().getAtime())));
			passed = false;
		}
		
		if(inode.getStat().getCtime() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have ctime 0; had %d",
							inode.getStat().getCtime())));
			passed = false;
		}
		
		if(inode.getStat().getMtime() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have mtime 0; had %d",
							inode.getStat().getMtime())));
			passed = false;
		}
		
		if(inode.getStat().getSize() != 0) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected deleted inode to have size 0; had %d",
							inode.getStat().getSize())));
			passed = false;
		}
		
		if(inode.getStat().getInodeId() != 0 && expectedId < fs.getInodeTable().nextInodeId()) {
			if(inode.getStat().getInodeId() != expectedId) {
				issues.add(new IntegrityIssueInode(inode, expectedId,
						String.format("Expected deleted inode to have inodeId %d; got %d",
								expectedId,
								inode.getStat().getInodeId())));
				passed = false;
			}
		}
		
		return passed;
	}
	
	protected boolean validateActiveUserInode(LinkedList<IntegrityIssue> issues, Inode inode, long expectedId) {
		boolean passed = true;
		
		if(inode.getStat().getInodeId() != expectedId) {
			issues.add(new IntegrityIssueInode(inode, expectedId,
					String.format("Expected inode to have ID %d; had %d",
							expectedId,
							inode.getStat().getInodeId())));
			passed = false;
		}
		
		passed &= validateLinkCount(issues, inode);
		passed &= validateRefTag(issues, inode);
		passed &= validateChangedFrom(issues, inode);
		passed &= validateFlags(issues, inode);
		passed &= validateIdentity(issues, inode);
		passed &= validateStat(issues, inode);
		passed &= validateNotInFreelist(issues, inode);
		
		if(inode.getStat().isDirectory()) {
			passed &= validateDirectory(issues, inode);
		}
		
		return passed;
	}
	
	protected boolean validateLinkCount(LinkedList<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		
		int expectedNlink = linkCounts.getOrDefault(inode.getStat().getInodeId(), 0);
		if(inode.getNlink() != expectedNlink) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Expected nlink %d, got %d",
							expectedNlink,
							inode.getNlink())));
			passed = false;
		}
		
		return passed;
	}
	
	protected boolean validateRefTag(LinkedList<IntegrityIssue> issues, Inode inode) {
		int expectedRefType, expectedNumPages;
		boolean passed = true;
		
		if(inode.getStat().getSize() < fs.getArchive().getCrypto().hashLength()) {
			expectedRefType = RefTag.REF_TYPE_IMMEDIATE;
			expectedNumPages = 1;
		} else if(inode.getStat().getSize() <= fs.getArchive().getConfig().getPageSize()) {
			expectedRefType = RefTag.REF_TYPE_INDIRECT;
			expectedNumPages = 1;
		} else {
			expectedRefType = RefTag.REF_TYPE_2INDIRECT;
			expectedNumPages = (int) Math.ceil(((double) inode.getStat().getSize()) / fs.getArchive().getConfig().getPageSize());
		}
		
		if(inode.getRefTag().getRefType() != expectedRefType) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Expected inode reftag %s, size %d to have type %d; had %d",
							Util.formatRefTag(inode.getRefTag()),
							inode.getStat().getSize(),
							expectedRefType,
							inode.getRefTag().getRefType())));
			passed = false;
		}
		
		if(inode.getRefTag().getNumPages() != expectedNumPages) {
			if(!(inode.getRefTag().getNumPages() == 0 && inode.getStat().getSize() == 0)) {
				issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
						String.format("Expected inode reftag %s, size %d to have %d pages; had %d",
								Util.formatRefTag(inode.getRefTag()),
								inode.getStat().getSize(),
								expectedNumPages,
								inode.getRefTag().getNumPages())));
				passed = false;
			}
		}
		
		if(inode.getStat().isRegularFile()) {
			long totalLength = 0;
			byte[] buf = new byte[fs.getArchive().getConfig().getPageSize()];

			try(ZKFile file = fs.open(inode, File.O_RDONLY)) {
				int readLen = 0;
				do {
					readLen = file.read(buf, 0, buf.length);
					totalLength += Math.max(readLen, 0);
				} while(readLen > 0);
			} catch (IOException exc) {
				issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
						String.format("Caught exception %s reading inode contents: %s",
								exc.getClass().getSimpleName(),
								exc.getMessage())));
				passed = false;
			}

			if(totalLength != inode.getStat().getSize()) {
				issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
						String.format("Expected %d bytes in file, read %d",
								inode.getStat().getSize(),
								totalLength)));
				passed = false;
			}
		}
		
		return passed;
	}
	
	protected boolean validateChangedFrom(LinkedList<IntegrityIssue> issues, Inode inode) {
		// wait until we add as lastInodeId field to the inode to validate this...
		return true;
	}
	
	protected boolean validateFlags(LinkedList<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		
		if(inode.getFlags() != 0) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Expected flags 0x00, got 0x%02x", inode.getFlags())));
			passed = false;
		}
		
		return passed;
	}
	
	protected boolean validateIdentity(LinkedList<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		
		if(inode.getIdentity() == 0) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Expected non-zero identity, got 0x%02x", inode.getIdentity())));
			passed = false;
		}

		int numReferences = identityCounts.getOrDefault(inode.getIdentity(), 0);
		if(numReferences != 1) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Expected exactly 1 reference to identity %016x in table, got %d",
							inode.getIdentity(),
							numReferences)));
			passed = false;
		}

		return passed;
	}
	
	protected boolean validateStat(LinkedList<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		Stat stat = inode.getStat();
		
		if(stat.getSize() < 0) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Expected positive size, got %d",
							stat.getSize())));
			passed = false;
		}
		
		if(stat.isFifo() || stat.isDevice()) {
			if(stat.getSize() != 0) {
				issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
						String.format("Expected size 0 for inode of type %d, got %d",
								stat.getType(),
								stat.getSize())));
				passed = false;
			}
		}

		return passed;
	}
	
	protected boolean validateNotInFreelist(LinkedList<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		
		if(freelistContents.contains(inode.getStat().getInodeId())) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Listed in freelist, nlink = %d", inode.getNlink())));
			passed = false;
		}
		
		return passed;
	}
	
	protected boolean validateInFreelist(LinkedList<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		
		if(!freelistContents.contains(inode.getStat().getInodeId())) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Not listed in freelist, nlink = %d", inode.getNlink())));
			passed = false;
		}
		
		return passed;
	}
	
	protected boolean validateDirectory(LinkedList<IntegrityIssue> issues, Inode inode) {
		boolean passed = true;
		
		try(ZKDirectory dir = new ZKDirectory(fs, inode)) {
			// ensure we can open and list the directory
			dir.list();
			for(String path : dir.entries.keySet()) {
				Long inodeId = dir.entries.get(path);
				if(inodeId == null) {
					issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
							String.format("Directory %d %016x had path %s with null inodeId",
								inode.getStat().getInodeId(),
								inode.getIdentity(),
								path
							)));
					passed = false;
				} else if(inodeId < 0) {
					issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
							String.format("Directory %d %016x had path %s with negative inodeId %d",
								inode.getStat().getInodeId(), inode.getStat().getInodeId(),
								inode.getIdentity(),
								path,
								inodeId
							)));
					passed = false;
				} else if(inodeId < InodeTable.USER_INODE_ID_START && inodeId != InodeTable.INODE_ID_ROOT_DIRECTORY) {
					issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
							String.format("Directory %d %016x had path %s with reserved inodeId %d",
								inode.getStat().getInodeId(),
								inode.getIdentity(),
								path,
								inodeId
							)));
					passed = false;
				} else if(inodeId >= fs.getInodeTable().nextInodeId()) {
					issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
							String.format("Directory %d %016x had path %s with inodeId %d; exceeds expected nextInodeId %d",
								inode.getStat().getInodeId(),
								inode.getIdentity(),
								path,
								inodeId,
								fs.getInodeTable().nextInodeId()
							)));
					passed = false;
				}
			}
		} catch (Exception exc) {
			issues.add(new IntegrityIssueInode(inode, inode.getStat().getInodeId(),
					String.format("Caught exception %s opening directory: %s",
							exc.getClass().getSimpleName(),
							exc.getMessage())
					));
			passed = false;
		}
		
		return passed;
	}
	
	protected void scan() throws IOException {
		scanPaths();
		scanInodes();
		scanFreelist();
	}

	protected void scanPaths() throws IOException {
		try(ZKDirectory dir = fs.opendir("/")) {
			dir.walk(Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS|Directory.LIST_OPT_INCLUDE_DOT_DOTDOT,
				(path, lstat, isBroken, parent)->
			{
				long inodeId = lstat.getInodeId();
				linkCounts.put(inodeId, 1 + linkCounts.getOrDefault(inodeId, 0));
			});
		} catch(Exception exc) {}
	}
	
	protected void scanInodes() throws IOException {
		for(long inodeId = InodeTable.USER_INODE_ID_START; inodeId < fs.getInodeTable().nextInodeId(); inodeId++) {
			Inode inode = fs.getInodeTable().inodeWithId(inodeId);
			identityCounts.put(inode.getIdentity(), 1 + identityCounts.getOrDefault(inode.getIdentity(), 0));
		}
	}
	
	protected void scanFreelist() throws IOException {
		try(FreeList dupeList = new FreeList(fs.getInodeTable().freelist.inode)) {
			try {
				while(true) dupeList.loadNextPage();
			} catch(FreeListExhaustedException exc) {}
			
			freelistContents = new LinkedList<>(dupeList.available);
		}
	}
}
