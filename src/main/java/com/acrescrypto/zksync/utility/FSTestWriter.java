package com.acrescrypto.zksync.utility;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;

import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.crypto.PRNG;
import com.acrescrypto.zksync.exceptions.ActUnavailableException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;

public class FSTestWriter {
	FS fs;	
	RandomActor actor;
	PRNG prng;
	boolean printLog = true;
	String name;
	
	public FSTestWriter(FS fs, long nonce, String name) {
		this(fs, nonce, 0, name);
	}
	
	public FSTestWriter(FS fs, long nonce, int offset, String name) {
		this.fs = fs;
		this.name = name;
		this.prng = CryptoSupport.defaultCrypto().prng(Util.serializeLong(nonce));
		actor = new RandomActor(prng);
		burn(offset);
		setupActor();
	}
	
	public void burn(int count) {
		for(int i = 0; i < count; i++) {
			prng.getInt();
		}
	}
	
	public void setupActor() {
		actor.addAction(10, ()->makeDirectory());
		actor.addAction(5, ()->rmrfDirectory());
		actor.addAction(2, ()->createSymlink());
		actor.addAction(1, ()->deleteSymlink());
		actor.addAction(10, ()->createImmediateFile());
		actor.addAction(10, ()->create1pageFile());
		actor.addAction(10, ()->createMultipageFile());
		actor.addAction(30, ()->modifyFile());
		actor.addAction(20, ()->truncateFile());
		actor.addAction(20, ()->extendFile());
		actor.addAction(20, ()->deleteFile());
	}
	
	public void act() throws IOException {
		actor.act();
	}
	
	protected int immediateSize() {
		if(fs instanceof ZKFS) {
			return ((ZKFS) fs).getArchive().getCrypto().hashLength();
		} else {
			return CryptoSupport.defaultCrypto().hashLength() - 1;
		}
	}
	
	protected int pageSize() {
		if(fs instanceof ZKFS) {
			ZKFS zkfs = (ZKFS) fs;
			return zkfs.getArchive().getConfig().getPageSize();
		} else {
			return ZKArchive.DEFAULT_PAGE_SIZE;
		}
	}
	
	protected void log(String msg) {
		if(printLog) {
			Util.debugLog("FSTestWriter " + name + ": " + msg);
		}
	}
	
	protected void needsNotNull(String path) {
		if(path == null) throw new ActUnavailableException();
	}
	
	protected <T> T pickFromCollection(Collection<T> collection) {
		if(collection.isEmpty()) return null;
		
		int offset = prng.getInt(collection.size()), index = 0;
		for(T item : collection) {
			if(index++ == offset) return item;
		}
		
		return null;
	}
	
	protected String randomString(int length) {
		String s = "";
		String chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
		
		while(s.length() < length) {
			s += chars.charAt(prng.getInt(chars.length()));
		}
		
		return s;
	}
	
	protected String randomName() {
		return name + "-" + randomString(8);
	}
	
	protected String makePath() throws IOException {
		while(true) {
			String path = Paths.get(pickExistingDirectory(), randomName()).toString();
			if(fs.exists(path)) continue;
			return path;
		}
	}
	
	protected byte[] makeContent(int length) {
		return prng.getBytes(length);
	}

	protected String pickExistingFile() throws IOException {
		try(Directory dir = fs.opendir("/")) {
			int opts = Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS | Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS;
			LinkedList<String> files = new LinkedList<>();
			dir.walk(opts, (path, stat, isBrokenSymlink, parent)->{
				if(!stat.isRegularFile()) return;
				files.add(path);
			});
			
			return pickFromCollection(files);
		}
	}
	
	protected String pickExistingLink() throws IOException {
		try(Directory dir = fs.opendir("/")) {
			LinkedList<String> symlinks = new LinkedList<>();
			int opts = Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS | Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS;
			dir.walk(opts,
				(path, stat, isBrokenSymlink, parent)->{
					if(stat.isSymlink()) {
						symlinks.add(path);
					}
				});
			return pickFromCollection(symlinks);
		}
	}
	
	protected String pickExistingDirectory() throws IOException {
		try(Directory dir = fs.opendir("/")) {
			LinkedList<String> directories = new LinkedList<>();
			int opts = Directory.LIST_OPT_DONT_FOLLOW_SYMLINKS;
			dir.walk(opts,
				(path, stat, isBrokenSymlink, parent)->{
					if(stat.isDirectory()) {
						directories.add(path);
					}
				});
			if(directories.isEmpty()) return "/";
			return pickFromCollection(directories);
		}
	}
	
	protected void makeDirectory() throws IOException {
		String path = makePath() + "-dir";
		log("mkdir " + path);
		fs.mkdir(path);
	}
	
	protected void rmrfDirectory() throws IOException {
		String path = pickExistingDirectory();
		if(path.equals("/")) throw new ActUnavailableException();
		needsNotNull(path);
		log("rmrf " + path);
		fs.rmrf(path);
	}
	
	protected void createSymlink() throws IOException {
		String target = pickExistingFile();
		String path = makePath() + "-sym";
		needsNotNull(path);
		needsNotNull(target);
		
		log("symlink " + path + " -> " + target);
		fs.symlink(target, path);
	}
	
	protected void deleteSymlink() throws IOException {
		String path = pickExistingLink();
		needsNotNull(path);
		log("rm " + path + " # symlink");
		fs.unlink(path);
	}
	
	protected void createImmediateFile() throws IOException {
		String path = makePath() + "-0pf";
		int len = prng.getInt(immediateSize());
		log("create " + path + " # immediate, " + len);
		fs.write(path, makeContent(len));
	}
	
	protected void create1pageFile() throws IOException {
		String path = makePath() + "-1pf";
		int len = prng.getInt(pageSize());
		log("create " + path + " # 1-page, " + len);
		fs.write(path, makeContent(len));
	}
	
	protected void createMultipageFile() throws IOException {
		String path = makePath() + "-mpf";
		int len = prng.getInt(pageSize()) + pageSize() + 1;
		log("create " + path + " # multipage, " + len);
		fs.write(path, makeContent(len));
	}
	
	protected void modifyFile() throws IOException {
		String path = pickExistingFile();
		needsNotNull(path);
		try(File file = fs.open(path, File.O_WRONLY)) {
			long offset = prng.getLong(file.getSize() + 1);
			int maxLength = Math.max(1, (int) (file.getStat().getSize() - offset));
			int length = prng.getInt(maxLength);
			
			log("modify " + path + " len=" + length + " offset=" + offset);
			file.seek(offset, File.SEEK_SET);
			file.write(prng.getBytes(length)); 
		}
	}
	
	protected void truncateFile() throws IOException {
		String path = pickExistingFile();
		needsNotNull(path);
		if(fs.stat(path).getSize() == 0) throw new ActUnavailableException(); 
		long newSize = prng.getLong(fs.stat(path).getSize());
		
		log("truncate " + path + " newSize=" + newSize);
		fs.truncate(path, newSize);
	}
	
	protected void extendFile() throws IOException {
		String path = pickExistingFile();
		needsNotNull(path);
		
		int len = prng.getInt(2*pageSize());
		byte[] content = prng.getBytes(len);
		log("extend " + path + " extraContentLen=" + len);
		
		try(File file = fs.open(path, File.O_WRONLY|File.O_APPEND)) {
			file.write(content);
		}
	}
	
	protected void deleteFile() throws IOException {
		String path = pickExistingFile();
		needsNotNull(path);
		
		log("rm " + path);
		fs.unlink(path);
	}
}
