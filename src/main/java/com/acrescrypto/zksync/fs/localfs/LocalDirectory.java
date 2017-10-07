package com.acrescrypto.zksync.fs.localfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Set;

import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.fs.Directory;
import com.acrescrypto.zksync.fs.File;

public class LocalDirectory implements Directory {
	
	String path;
	
	LocalDirectory(String path) throws EISNOTDIRException {
		this.path = path;
		if(!Files.isDirectory(Paths.get(path))) throw new EISNOTDIRException(path + ": not a directory");
	}

	public String[] list() throws IOException {
		ArrayList<String> paths = new ArrayList<String>();
		for(Path entry: Files.newDirectoryStream(Paths.get(this.path))) {
			paths.add(entry.toString());
		}
		
		String[] retval = new String[paths.size()];
		for(int i = 0; i < paths.size(); i++) retval[i] = paths.get(i);
		return retval;
	}

	public Directory mkdir(String name) throws IOException {
		Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rwx-r-x---");
		FileAttribute<Set<PosixFilePermission>> attr = PosixFilePermissions.asFileAttribute(perms);
		Files.createDirectory(Paths.get(path), attr);
		return new LocalDirectory(this.path + "/" + name);
	}

	public void link(String path, File file) throws IOException {
		link(path, file.getPath());
	}
	
	public void link(String path, String dest) throws IOException {
		Files.createLink(Paths.get(path + "/" + path), Paths.get(dest));
	}

	public void unlink(String path) throws IOException {
		Files.delete(Paths.get(this.path + "/" + path));
	}
}
