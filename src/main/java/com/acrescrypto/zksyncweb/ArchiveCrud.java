package com.acrescrypto.zksyncweb;

import java.io.IOException;
import java.io.StringReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.stream.JsonParsingException;
import javax.ws.rs.core.MultivaluedMap;

import com.acrescrypto.zksync.exceptions.EEXISTSException;
import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.EISNOTDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.fs.DirectoryTraverser;
import com.acrescrypto.zksync.fs.DirectoryTraverser.TraversalEntry;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.Inode;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.PageTree.PageTreeStats;
import com.acrescrypto.zksync.fs.zkfs.ZKDirectory;
import com.acrescrypto.zksync.net.PageQueue;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.fs.zkfs.ZKFile;
import com.acrescrypto.zksyncweb.data.XAPIResponse;
import com.acrescrypto.zksyncweb.data.XPathStat;

public class ArchiveCrud {
	public static byte[] get(ZKFS fs, String path, Map<String, String> params) throws IOException, XAPIResponse {
		int existingPriority = Integer.MIN_VALUE;
		int priority = Integer.parseInt(params.getOrDefault("priority", "0"));
		boolean isStat = Boolean.parseBoolean(params.getOrDefault("stat", "false"));
		boolean isInode = Boolean.parseBoolean(params.getOrDefault("inode", "false"));
		boolean isInodeRaw = Boolean.parseBoolean(params.getOrDefault("inodeRaw", "false"));
		boolean isQueue = Boolean.parseBoolean(params.getOrDefault("queue", "false"));
		boolean isCancel = Boolean.parseBoolean(params.getOrDefault("cancel", "false"));
		boolean isNofollow = Boolean.parseBoolean(params.getOrDefault("nofollow", "false"));
		
		while(path.startsWith("//")) path = path.substring(1);

		try {
			// TODO Someday: (refactor) Make ?queue=true non-blocking even if we don't have inode table or directory
			/* (and no fair using a background thread, either -- these requests can really pile up!) */
			Stat stat;
			long inodeId;
			
			if(isInode) {
				while(path.startsWith("/")) path = path.substring(1);
				if(!path.matches("^\\d+$")) throw XAPIResponse.withError(400, "Path must be numeric inode ID when inode=true");
				inodeId = Integer.parseInt(path);
				stat = fs.getInodeTable().inodeWithId(inodeId).getStat();
			} else {
				stat = fs.stat(path);
				inodeId = stat.getInodeId();
			}
			
			if(isCancel) {
				// ?cancel=true causes us to remove the path from the download queue, if already present
				fs.getArchive().getConfig().getSwarm().cancelInode(fs.getBaseRevision(), inodeId);
				throw XAPIResponse.successResponse();
			}
			
			existingPriority = fs.getArchive().getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId);
			if(isStat) {
				// ?stat=true causes us to return stat information instead of contents
				// add in ?nofollow=true to do lstat
				Inode inode = fs.getInodeTable().inodeWithId(inodeId);
				PageTree tree = new PageTree(inode);
				throw XAPIResponse.withPayload(new XPathStat(fs, path, inode, tree.getStats(), existingPriority, !isNofollow, 0));
			}
			
			if(params.containsKey("priority") || existingPriority == PageQueue.CANCEL_PRIORITY) {
				// ?priority=1234 tells us to request the file from the swarm with a specific priority.
				// if there existing priority is CANCEL_PRIORITY, it's not queued yet and fall back on default priority if not supplied
				// (but don't overwrite an existing priority if we have one and no new priority was explicitly set)
				fs.getArchive().getConfig().getSwarm().requestInode(priority, fs.getBaseRevision(), inodeId);
			}
			
			if(isQueue) {
				// TODO Someday: (refactor) ?queue=true&recursive=true should allow non-blocking queue of all subpaths of requested directory
				// ?queue=true causes non-blocking return with no data (just ensure we grab this from the swarm)
				throw XAPIResponse.successResponse();
			}
			
			long offset = Long.parseLong(params.getOrDefault("offset", "0"));
			int length = Integer.parseInt(params.getOrDefault("length", "-1"));
			long size = stat.getSize();
			
			if(length < 0) {
				length = (int) (size - offset + length + 1);
			}
			
			length = (int) Math.min(size-offset, length);
			
			ZKFile file = null;
			try {
				int mode = File.O_RDONLY;
				if(isNofollow) {
					mode |= File.O_NOFOLLOW | ZKFile.O_LINK_LITERAL;
				}
				
				if(isInode) {
					// direct open using new ZKFile avoids EISDIR
					file = new ZKFile(fs, fs.getInodeTable().inodeWithId(inodeId), mode, true);
				} else {
					file = fs.open(path, mode);
				}
				
				if(isInodeRaw) {
					return file.getInode().serialize();
				} else {
					byte[] data = new byte[length];
					file.seek(offset, File.SEEK_SET);
					file.read(data, 0, length);
					return data;
				}
			} finally {
				if(file != null) {
					file.close();
				}
			}
		} catch(ENOENTException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		} catch(EISDIRException exc) {
			boolean isRecursive = Boolean.parseBoolean(params.getOrDefault("recursive", "false"));
			boolean isListStat = Boolean.parseBoolean(params.getOrDefault("liststat", "false"));
			boolean dotdirs = Boolean.parseBoolean(params.getOrDefault("dotdirs", "false"));
			
			LinkedList<TraversalEntry> entries = new LinkedList<>();
			try(ZKDirectory dir = fs.opendir(path)) {
				DirectoryTraverser traverser = new DirectoryTraverser(fs, dir);
				traverser.setRecursive(isRecursive);
				traverser.setHideDirectories(false);
				traverser.setIncludeDotDirs(dotdirs);
				while(traverser.hasNext()) {
					entries.add(traverser.next());
				}
			}
			
			Map<String,Object> payload = new HashMap<>(), fsMap = new HashMap<>();
			payload.put("fs", fsMap);
			fsMap.put("revtag", fs.getBaseRevision());
			fsMap.put("dirty", fs.isDirty());
			
			if(isListStat) {
				// ?liststat=true causes stat information to be included with directory listings
				ArrayList<XPathStat> pathStats = new ArrayList<>(entries.size());
				for(TraversalEntry entry : entries) {
					Inode inode;
					try {
						inode = fs.inodeForPath(entry.getPath(), !isNofollow);
					} catch(ENOENTException exc2) {
						/* we hit a broken symlink. we don't want to leave it out, but we need a stat,
						 * and so we need to take the stat of the sylink itself regardless of what was requested.
						 */
						inode = fs.inodeForPath(entry.getPath(), false);
					}
					PageTreeStats treeStat = new PageTree(inode).getStats();
					
					pathStats.add(new XPathStat(fs,
							entry.getPath(),
							inode,
							treeStat,
							existingPriority,
							!isNofollow,
							0));
				}
				
				payload.put("entries", pathStats);
			} else {
				ArrayList<String> paths = new ArrayList<>(entries.size());
				for(TraversalEntry entry : entries) {
					paths.add(entry.getPath());
				}
				payload.put("entries", paths);
			}
			throw XAPIResponse.withPayload(payload);
		}
	}
	
	public static XAPIResponse post(ZKFS fs, String path, Map<String, String> params, byte[] contents) throws IOException {
		while(path.startsWith("//")) path = path.substring(1);
		
		long offset = Long.parseLong(params.getOrDefault("offset", "0"));
		boolean truncate = Boolean.parseBoolean(params.getOrDefault("truncate", "true"));
		String user = params.getOrDefault("user", null);
		String group = params.getOrDefault("group", null);
		int uid = Integer.parseInt(params.getOrDefault("uid", "-1"));
		int gid = Integer.parseInt(params.getOrDefault("gid", "-1"));
		long atime = Long.parseLong(params.getOrDefault("atime", "-1"));
		long mtime = Long.parseLong(params.getOrDefault("mtime", "-1"));
		boolean isDir = Boolean.parseBoolean(params.getOrDefault("isDir", "false"));
		int mode;
		
		if(params.getOrDefault("mode", "-1").startsWith("0")) {
			// octal
			mode = Integer.parseInt(params.getOrDefault("mode", "-1"), 8);
		} else {
			// we COULD handle hex, but who sets modes in hex?
			mode = Integer.parseInt(params.getOrDefault("mode", "-1"));
		}

		try {
			if(isDir) {
				fs.mkdir(path);
			} else {
				try(ZKFile file = fs.open(path, File.O_WRONLY|File.O_CREAT)) {
					if(offset < 0) {
						offset = file.getSize() - offset + 1;
					}
					
					file.seek(offset, File.SEEK_SET);
					file.write(contents);
					if(truncate) {
						file.truncate(file.pos());
					}
				}
			}
		} catch(ENOENTException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		} catch(EISDIRException exc) {
			if(offset > 0 || truncate || contents.length > 0) {
				throw XAPIResponse.withError(409, "Is a directory");
			}
		} catch(EEXISTSException exc) {
			throw XAPIResponse.withError(409, "Path already exists");
		} catch(EISNOTDIRException exc) {
			throw XAPIResponse.withError(409, "Parent is not a directory");
		}
		
		if(user != null) {
			fs.chown(path, user);
		}
		
		if(uid >= 0) {
			fs.chown(path, uid);
		}
		
		if(group != null) {
			fs.chgrp(path, group);
		}
		
		if(gid >= 0) {
			fs.chgrp(path, gid);
		}
		
		if(mode >= 0) {
			fs.chmod(path, mode);
		}
		
		if(params.containsKey("mtime")) {
			fs.setMtime(path, mtime);
		}
		
		if(params.containsKey("atime")) {
			fs.setAtime(path, atime);
		}

		
		return XAPIResponse.successResponse();
	}
	
	public static XAPIResponse delete(ZKFS fs, String path, Map<String, String> params) throws IOException {
		while(path.startsWith("//")) path = path.substring(1);
		boolean recursive = Boolean.parseBoolean(params.getOrDefault("recursive", "false"));
		
		try {
			if(recursive) {
				fs.rmrf(path);
			} else {
				fs.unlink(path);
			}
		} catch(ENOENTException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		} catch(EISDIRException exc) {
			try {
				fs.rmdir(path);
			} catch(ENOTEMPTYException exc2) {
				throw XAPIResponse.withError(409, "Directory not empty");
			}
		} catch(EISNOTDIRException exc) {
			fs.unlink(path);
		}
		
		return XAPIResponse.successResponse();
	}
	
	public static XAPIResponse put(ZKFS fs, String path, Map<String, String> params, byte[] body) throws IOException {
		while(path.startsWith("//")) path = path.substring(1);
		JsonReader reader = Json.createReader(new StringReader(new String(body)));
		JsonObject json = null;
		
		try {
			json = reader.readObject();
		} catch(JsonParsingException exc) {
			throw XAPIResponse.withError(400, "Must supply JSON object as body");
		}
		
		if(!json.containsKey("type")) {
			throw XAPIResponse.withError(400, "must supply type field");
		}

		String source = json.getString("source", null);
		String type = json.getString("type");
		boolean followSymlinks = json.getBoolean("followSymlinks", true);
		
		// TODO: need nofollow and recursive
		
		try {
			switch(type) {
			case "copy":
				if(source == null) throw XAPIResponse.withError(400, "Must supply source field");
				fs.cp(source, path);
				break;
			case "hardlink":
				if(source == null) throw XAPIResponse.withError(400, "Must supply source field");
				if(fs.stat(source).isDirectory()) {
					throw XAPIResponse.withError(400, "Cannot create hard links to directories");
				}
				fs.link(source, path);
				break;
			case "symlink":
				if(source == null) throw XAPIResponse.withError(400, "Must supply source field");
				followSymlinks = false;
				fs.symlink(source, path);
				break;
			case "directory":
				boolean makeParents = json.getBoolean("parents", false);
				if(makeParents) {
					fs.mkdirp(path);
				} else {
					fs.mkdir(path);
				}
				break;
			case "fifo":
				fs.mkfifo(path);
				break;
			case "device":
				try {
					String device_type = json.getJsonString("device_type").getString();
					int major = json.getJsonNumber("major").intValueExact();
					int minor = json.getJsonNumber("minor").intValueExact();
					if(!device_type.equals("b") && !device_type.equals("c")) {
						throw XAPIResponse.withError(400, "device_type field must be 'b' or 'c'");
					}
					
					int devtype = device_type.equals("b") ? Stat.TYPE_BLOCK_DEVICE : Stat.TYPE_CHARACTER_DEVICE;
					fs.mknod(path, devtype, major, minor);
					break;
				} catch(ArithmeticException|ClassCastException exc) {
					throw XAPIResponse.withError(400, "Must supply string device_type, int32 major, int32 minor. device_type must be 'b' or 'c'.");
				}
			case "metadata":
				// no special action, just use the metadata stuff
				break;
			case "move":
				// TODO
			default:
				throw XAPIResponse.withError(400, "Invalid type: " + type);
			}
			
			if(json.containsKey("mode")) {
				try {
					fs.chmod(path, json.getInt("mode"), followSymlinks);
				} catch(ClassCastException exc) {
					String modeStr = json.getString("mode");
					int mode = Integer.parseInt(modeStr, 8);
					fs.chmod(path, mode, followSymlinks);
				}
			}
			
			if(json.containsKey("uid")) {
				fs.chown(path, json.getInt("uid"), followSymlinks);
			}
			
			if(json.containsKey("user")) {
				fs.chown(path, json.getString("user"), followSymlinks);
			}
			
			if(json.containsKey("gid")) {
				fs.chgrp(path, json.getInt("gid"), followSymlinks);
			}

			if(json.containsKey("group")) {
				fs.chgrp(path, json.getString("group"), followSymlinks);
			}

			if(json.containsKey("mtime")) {
				fs.setMtime(path, json.getJsonNumber("mtime").longValue(), followSymlinks);
			}
			
			if(json.containsKey("atime")) {
				fs.setAtime(path, json.getJsonNumber("atime").longValue(), followSymlinks);
			}
			
			if(json.containsKey("ctime")) {
				fs.setCtime(path, json.getJsonNumber("ctime").longValue(), followSymlinks);
			}
			
			throw XAPIResponse.successResponse();
		} catch(ENOENTException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		} catch(EEXISTSException exc) {
			throw XAPIResponse.withError(409, "Path already exists");
		} catch(ClassCastException exc) {
			throw XAPIResponse.withError(400, "Invalid parameter datatype");
		}
	}
	
	public static String basePath(String path) throws MalformedURLException {
		URL url = new URL("file://domain/" + path);
		String realPath = url.getPath();
		
		while(path.startsWith("/") && path.length() > 1) {
			path = path.substring(1);
		}
		
		if(realPath.endsWith("/") && path.length() > 1) {
			realPath = realPath.substring(0, realPath.length()-1);
		}
		
		return realPath;
	}
	
	public static Map<String, String> convertMultivaluedToSingle(MultivaluedMap<String, String> map) {
		Map<String, String> single = new LinkedHashMap<>();
		map.forEach((key, value)->{
			if(value.size() == 0) return;
			single.put(key, value.get(0));
		});
		
		return single;
	}
}
