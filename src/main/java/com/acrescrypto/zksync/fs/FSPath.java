package com.acrescrypto.zksync.fs;

import java.nio.file.Path;
import java.util.LinkedList;

import com.acrescrypto.zksync.utility.Util;

public class FSPath {
	protected class FSPathDissection {
		boolean            isAbsolute,
		                   hasTrailingDelimiter;
		LinkedList<String> components;
		String             driveLetter;
		String             sourcePlatform;
		
		public FSPathDissection(String path) {
			explode(path);
		}
		
		public FSPathDissection(FSPathDissection other) {
			this.isAbsolute           = other.isAbsolute;
			this.hasTrailingDelimiter = other.hasTrailingDelimiter;
			this.components           = new LinkedList<>(other.components);
			this.driveLetter          = other.driveLetter;
			this.sourcePlatform       = other.sourcePlatform;
		}

		protected void explode(String path) {
			String lower = path.toLowerCase();
			
			if       (lower.matches("^[a-z]:\\\\.*")) {
				fromWindows(path); // e.g. C:\
			} else if(lower.matches("^[a-z]:/.*")) {
				fromBastardWindows(path); // e.g. C:/
			} else if(lower.contains("/")) {
				fromPosix(path); // windows doesn't allow forward slashes
			} else if(lower.contains("\\")) {
				/* backslashes can appear in unix paths, but if there are no forward slashes
				 * safer to assume this is a relative windows path */
				fromWindows(path);
			} else {
				fromPosix(path); // just a filename, doesn't matter what we call it
			}
		}
		
		protected void fromWindows(String path, String delimiter) {
			String splitDelimiter         = delimiter.equals("\\")
					                        ? "\\\\+"
					                        : delimiter + "+";
			components                    = new LinkedList<>();
			String[] comps                = path.split   (splitDelimiter);
			hasTrailingDelimiter          = path.endsWith(delimiter);
			sourcePlatform                = "windows";
			
			if(comps[0].length() == 2 && comps[0].endsWith(":")) {
				isAbsolute = true;
				driveLetter = comps[0].substring(0, 1).toUpperCase();
			} else if(comps[0].length() == 0) {
				isAbsolute = true;
			}
			
			boolean first = true;
			for(String comp : comps) {
				if(   first
				   && driveLetter != null
				   && comp.toUpperCase().equals(driveLetter + ":")) {
					continue;
				}
				
				first = false;
				if(comp.length() == 0) continue;
				components.add(comp);
			}
		}
		
		protected void fromWindows(String path) {
			fromWindows(path, "\\");
		}
		
		protected void fromBastardWindows(String path) {
			fromWindows(path, "/");
		}
		
		protected void fromPosix(String path) {
			String[] comps                 = path.split      ("/+");
			isAbsolute                     = path.startsWith ("/" );
			hasTrailingDelimiter           = path.endsWith   ("/" );
			components                     = new LinkedList<>();
			sourcePlatform                 = "posix";
			
			for(String comp : comps) {
				if(comp.length() == 0) continue;
				components.add(comp);
			}
		}
	}
	
	public static FSPath join(String root, String... paths) {
		FSPath fsPath = new FSPath(root);
		
		for(String path : paths) {
			fsPath = fsPath.join(path);
		}
		
		return fsPath;
	}
	
	public static String standardize(String path) {
		return new FSPath(path).toPosix();
	}
	
	public static String standardize(Path path) {
		return new FSPath(path.toString()).toPosix();
	}
	
	public static String nativize(String path) {
		return new FSPath(path).toNative();
	}
	
	public static String nativize(Path path) {
		return new FSPath(path.toString()).toNative();
	}
	
	public static FSPath with(String path) {
		return new FSPath(path);
	}
	
	protected String             original;
	protected FSPathDissection   dissection;
	protected String             defaultDrive = "C";
	protected String             forcedPlatform;
	
	public FSPath(String path) {
		this.original       = path;
		this.dissection     = new FSPathDissection(path);
	}
	
	protected FSPath(FSPath path) {
		this.original       = path.original;
		this.dissection     = new FSPathDissection(path.dissection);
		this.defaultDrive   = path.defaultDrive;
		this.forcedPlatform = path.forcedPlatform;
	}
	
	public String drive() {
		return dissection.driveLetter != null
			   ? dissection.driveLetter
			   : defaultDrive();
	}
	
	public String defaultDrive() {
		return defaultDrive;
	}
	
	public FSPath defaultDrive(String defaultDrive) {
		this.defaultDrive = defaultDrive;
		return this;
	}
	
	public String sourcePlatform() {
		return dissection.sourcePlatform;
	}
	
	public String platform() {
		if(forcedPlatform != null) return forcedPlatform;
		return Util.isWindows()
		       ? "windows"
		       : "posix";
	}
	
	public FSPath platform(String platform) {
		this.forcedPlatform = platform;
		return this;
	}
	
	public FSPath join(String path) {
		return join(new FSPath(path));
	}
	
	public FSPath join(FSPath path) {
		StringBuilder sb = new StringBuilder();
		sb.append(toSourcePlatform());
		
		if(    dissection.isAbsolute
		    && dissection.driveLetter == null
		    && dissection.sourcePlatform.equals("windows"))
		{
			// implied drive letter
			sb = new StringBuilder(sb.toString().substring(2));
		}
		
		if(!dissection.hasTrailingDelimiter) sb.append("/");
		sb.append(path.toPlatform(dissection.sourcePlatform));
		
		return new FSPath(sb.toString()).platform(platform());
	}
	
	public FSPath normalize() {
		LinkedList<String> comps = new LinkedList<>();
		for(String element : dissection.components) {
			if(element.equals( ".")) continue;
			if(element.equals("..") && !comps.isEmpty()) {
				comps.removeLast();
				continue;
			}
			
			comps.add(element);
		}
		
		FSPath normalized                = new FSPath(this);
		normalized.dissection.components = comps;
		
		return normalized;
	}
	
	public String toNative() {
		return toPlatform(platform());
	}
		
	
	public String toPlatform(String platform) {
		switch(platform) {
		case "windows":
			return toWindows();
		case "posix":
			return toPosix();
		default:
			throw new RuntimeException("Unsupported platform " + platform());
		}
	}
	
	public String original() {
		return original;
	}
	
	public String toSourcePlatform() {
		return toPlatform(dissection.sourcePlatform);
	}
	
	public String toWindows() {
		StringBuilder sb = new StringBuilder();
		
		if(dissection.isAbsolute) {
			sb.append(drive() + ":");
		}
		
		for(String comp : dissection.components) {
			if(sb.length() != 0) {
				sb.append("\\");
			}
			
			sb.append(comp);
		}
		
		String str = sb.toString();
		if(dissection.hasTrailingDelimiter && !str.endsWith("\\")) {
			return str + "\\";
		}
		
		return str;
	}
	
	public String toPosix() {
		StringBuilder sb = new StringBuilder();
		if(dissection.isAbsolute) {
			sb.append("/");
			if(   dissection.sourcePlatform.equals("windows")
			   && dissection.driveLetter != null)
			{
				sb.append(drive());
			}
		}
		
		for(String comp : dissection.components) {
			if(sb.length() >= 2 || (sb.length() == 1 && !sb.toString().equals("/"))) {
				sb.append("/");
			}
			
			sb.append(comp);
		}
		
		String str = sb.toString();
		if(dissection.hasTrailingDelimiter && !str.endsWith("/")) {
			return str + "/";
		}
		
		return str;
	}
	
	public boolean descendsFrom(String path) {
		return descendsFrom(new FSPath(path));
	}
	
	public boolean descendsFrom(FSPath path) {
		if(path.dissection.components.size() > dissection.components.size()) return false;
		for(int i = 0; i < path.dissection.components.size(); i++) {
			String ours   = dissection.components.get(i);
			String theirs = path.dissection.components.get(i);
			if(!ours.equals(theirs)) return false;
		}
		
		return true;
	}
	
	public String toString() {
		return toPosix();
	}
}
