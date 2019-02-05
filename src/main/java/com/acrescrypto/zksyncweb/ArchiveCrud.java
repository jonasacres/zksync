package com.acrescrypto.zksyncweb;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.ws.rs.core.MultivaluedMap;

import com.acrescrypto.zksync.exceptions.EISDIRException;
import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.exceptions.ENOTEMPTYException;
import com.acrescrypto.zksync.fs.File;
import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.PageTree.PageTreeStats;
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
		boolean isQueue = Boolean.parseBoolean(params.getOrDefault("queue", "false"));
		boolean isCancel = Boolean.parseBoolean(params.getOrDefault("cancel", "false"));

		try {
			// TODO Someday: (refactor) Make ?queue=true non-blocking even if we don't have inode table or directory
			/* (and no fair using a background thread, either -- these requests can really pile up!) */
			Stat stat = fs.stat(path);
			long inodeId = stat.getInodeId();
			
			if(isCancel) {
				// ?cancel=true causes us to remove the path from the download queue, if already present
				fs.getArchive().getConfig().getSwarm().cancelInode(fs.getBaseRevision(), inodeId);
				throw XAPIResponse.successResponse();
			}
			
			existingPriority = fs.getArchive().getConfig().getSwarm().priorityForInode(fs.getBaseRevision(), inodeId);
			if(isStat) {
				// ?stat=true causes us to return stat information instead of contents
				PageTree tree = new PageTree(fs.inodeForPath(path));
				throw XAPIResponse.withPayload(new XPathStat(path, stat, tree.getStats(), existingPriority));
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
			long size = fs.stat(path).getSize();
			
			if(length < 0) {
				length = (int) (size - offset + length + 1);
			}
			
			length = (int) Math.min(size-offset, length);
			ZKFile file = fs.open(path, File.O_RDONLY);
			byte[] data = new byte[length];
			file.seek(offset, File.SEEK_SET);
			file.read(data, 0, length);
			file.close();
			
			return data;
		} catch(ENOENTException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		} catch(EISDIRException exc) {
			String[] listings = null;
			boolean isRecursive = Boolean.parseBoolean(params.getOrDefault("recursive", "false"));
			boolean isListStat = Boolean.parseBoolean(params.getOrDefault("liststat", "false"));
			
			if(isRecursive) {
				listings = fs.opendir(path).listRecursive();
			} else {
				listings = fs.opendir(path).list();
			}
			
			if(isListStat) {
				// ?liststat=true causes stat information to be included with directory listings
				ArrayList<XPathStat> pathStats = new ArrayList<>(listings.length);
				for(String subpath : listings) {
					String fqSubpath = path + "/" + subpath;
					Stat stat = fs.stat(fqSubpath);
					PageTreeStats treeStat = new PageTree(fs.inodeForPath(fqSubpath)).getStats();
					pathStats.add(new XPathStat(subpath, stat, treeStat, existingPriority));
				}
				
				throw XAPIResponse.withWrappedPayload("entries", pathStats);
			} else {
				throw XAPIResponse.withWrappedPayload("entries", listings);
			}
		}
	}
	
	public static XAPIResponse post(ZKFS fs, String path, Map<String, String> params, byte[] contents) throws IOException {
		long offset = Long.parseLong(params.getOrDefault("offset", "0"));
		boolean truncate = Boolean.parseBoolean(params.getOrDefault("truncate", "true"));
		String user = params.getOrDefault("user", null);
		String group = params.getOrDefault("group", null);
		int uid = Integer.parseInt(params.getOrDefault("uid", "-1"));
		int gid = Integer.parseInt(params.getOrDefault("gid", "-1"));
		int mode;
		if(params.getOrDefault("mode", "-1").startsWith("0")) {
			// octal
			mode = Integer.parseInt(params.getOrDefault("mode", "-1"), 8);
		} else {
			// we COULD handle hex, but who sets modes in hex?
			mode = Integer.parseInt(params.getOrDefault("mode", "-1"));
		}

		try {
			ZKFile file = fs.open(path, File.O_WRONLY|File.O_CREAT);
			file.seek(offset, File.SEEK_SET);
			file.write(contents);
			if(truncate) {
				file.truncate(file.pos());
			}
			file.close();
		} catch(ENOENTException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		} catch(EISDIRException exc) {
			if(offset > 0 || truncate || contents.length > 0) {
				throw XAPIResponse.withError(409, "Is a directory");
			}
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
		
		return XAPIResponse.successResponse();
	}
	
	public static XAPIResponse delete(ZKFS fs, String path, Map<String, String> params) throws IOException {
		try {
			fs.unlink(path);
		} catch(ENOENTException exc) {
			throw XAPIResponse.notFoundErrorResponse();
		} catch(EISDIRException exc) {
			try {
				fs.rmdir(path);
			} catch(ENOTEMPTYException exc2) {
				throw XAPIResponse.withError(409, "Directory not empty");
			}
		}
		
		return XAPIResponse.successResponse();
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