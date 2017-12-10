package com.acrescrypto.zksync.fs.sshfs;

import java.io.IOException;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.Stat;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.common.IOUtils;
import net.schmizz.sshj.connection.ConnectionException;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.TransportException;

public class SSHFS extends FS {
	SSHClient ssh;
	Session session;
	HashMap<String,HashMap<Integer,String>> osCommandMap = new HashMap<String,HashMap<Integer,String>>(); 
	
	int hostType = HOST_TYPE_UNPROBED;
	String user;
	String hostname;
	String remotePath;
	
	public final static int HOST_TYPE_UNPROBED = Integer.MAX_VALUE;
	public final static int HOST_TYPE_UNKNOWN = -1;
	public final static int HOST_TYPE_LINUX = 0;
	public final static int HOST_TYPE_OSX = 1;
	
	public SSHFS(String url) throws IOException {
		parseUrl(url);
		ssh = new SSHClient();
		ssh.loadKnownHosts();
		ssh.connect(url);
		// TODO: keep the connection alive for 60m idle, reconnect on demand
	}
	
	public void close() throws IOException {
		ssh.close();
	}

	@Override
	public Stat stat(String path) throws IOException {
		return parseStat(new String(execAndCheck("stat", "-L " + qualifiedPath(path))));
	}

	@Override
	public Stat lstat(String path) throws IOException {
		return parseStat(new String(execAndCheck("stat", qualifiedPath(path))));
	}

	@Override
	public SSHDirectory opendir(String path) throws IOException {
		return new SSHDirectory(this, path);
	}

	@Override
	public void mkdir(String path) throws IOException {
		execAndCheck("mkdir", "\"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void mkdirp(String path) throws IOException {
		execAndCheck("mkdir", "-p \"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void rmdir(String path) throws IOException {
		execAndCheck("rmdir", "\"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void unlink(String path) throws IOException {
		execAndCheck("rm", "-f \"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void link(String target, String link) throws IOException {
		execAndCheck("ln", "\"" + qualifiedPath(target) + "\" \"" + qualifiedPath(link) + "\"");
	}

	@Override
	public void symlink(String target, String link) throws IOException {
		execAndCheck("ln", "-s \"" + qualifiedPath(target) + "\" \"" + qualifiedPath(link) + "\"");
	}

	@Override
	public String readlink(String link) throws IOException {
		return trimResult(execAndCheck("readlink", "\"" + qualifiedPath(link) + "\""));
	}

	@Override
	public void mknod(String path, int type, int major, int minor) throws IOException {
		String typeChars[] = { "c", "b", "p" };
		if(type < 0 || type >= typeChars.length) throw new IllegalArgumentException();
		
		privilegedExecAndCheck("mknod", String.format("%s \"%s\" %d %d",
				typeChars[type],
				qualifiedPath(path),
				major,
				minor));
	}

	@Override
	public void mkfifo(String path) throws IOException {
		execAndCheck("mkfifo", "\"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void chmod(String path, int mode) throws IOException {
		execAndCheck("chmod", "0" + Integer.toString(mode,8) + " \"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void chown(String path, int uid) throws IOException {
		privilegedExecAndCheck("chown", uid + " \"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void chown(String path, String user) throws IOException {
		privilegedExecAndCheck("chown", user + " \"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void chgrp(String path, int gid) throws IOException {
		execAndCheck("chgrp", gid + " \"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void chgrp(String path, String group) throws IOException {
		execAndCheck("chgrp", group + " \"" + qualifiedPath(path) + "\"");
	}

	@Override
	public void setMtime(String path, long mtime) throws IOException {
		execAndCheck("touch", "-t " + timestampToTouchFormat(mtime) + " " + qualifiedPath(path));
	}

	@Override
	public void setCtime(String path, long ctime) throws IOException {
		throw new UnsupportedOperationException("can't set ctime on filesystem");
	}

	@Override
	public void setAtime(String path, long atime) throws IOException {
		execAndCheck("touch", "-at " + timestampToTouchFormat(atime) + " " + qualifiedPath(path));
	}

	@Override
	public void write(String path, byte[] contents) throws IOException {
		SSHMemFile memfile = new SSHMemFile(); // TODO: right constructor?
		memfile.setContents(contents);
		ssh.newSCPFileTransfer().download(qualifiedPath(path), memfile);
	}

	@Override
	public byte[] read(String path) throws IOException {
		SSHMemFile memfile = new SSHMemFile(); // TODO: right constructor?
		ssh.newSCPFileTransfer().download(qualifiedPath(path), memfile);
		return memfile.contents();
	}

	@Override
	public SSHFile open(String path, int mode) throws IOException {
		return new SSHFile(this, path, mode);
	}

	@Override
	public void truncate(String path, long size) throws IOException {
		execAndCheck("truncate", " -s " + size + " " + qualifiedPath(path));
	}
	
	public int getHostType() throws IOException {
		if(hostType != HOST_TYPE_UNPROBED) return hostType;
		return hostType = discoverHostType();
	}
	
	protected String oscmd(String cmd) throws IOException {
		return osCommandMap
		  .getOrDefault(cmd, new HashMap<Integer,String>())
		  .getOrDefault(getHostType(), cmd);
	}
	
	protected int discoverHostType() throws IOException {
		String uname = trimResult(execAndCheck("uname"));
		if(uname.equals("Linux")) return HOST_TYPE_LINUX;
		if(uname.equals("Darwin")) return HOST_TYPE_OSX;
		return HOST_TYPE_UNKNOWN;
	}
	
	protected String qualifiedPath(String path) {
		return Paths.get(remotePath, path).toString();
	}
	
	protected String timestampToTouchFormat(long timestamp) {
		return (new SimpleDateFormat("yyyyMMddhhmm.ss")).format(new Date(timestamp/(1000l*1000l)));
	}
	
	// TODO: all this command execution stuff should be its own class
	protected void privilegedExecAndCheck(String command, String args) throws IOException {
		execAndCheck("sudo", oscmd(command) + " " + args);
	}
	
	protected byte[] execAndCheck(String command) throws IOException {
		return execAndCheck(command, null, null);
	}
	
	protected byte[] execAndCheck(String command, String args) throws IOException {
		return execAndCheck(command, null, null);
	}
	
	protected byte[] execAndCheck(String command, String args, byte[] stdin) throws IOException {
		try {
			String cmdline = String.format("%s%s", oscmd(command), args == null ? "" : " " + args);
			Command cmd = session.exec(cmdline);
			cmd.getOutputStream().write(stdin);
			byte[] result = IOUtils.readFully(cmd.getInputStream()).toByteArray();
			cmd.join(5, TimeUnit.SECONDS);
			if(cmd.getExitStatus() != 0) throw new IOException("Command failed: " + command);
			return result;
		} catch (ConnectionException | TransportException e) {
			throw new IOException("Caught exception executing " + command);
		}
	}
	
	protected String trimResult(byte[] result) {
		return (new String(result)).replaceAll("^\\s+", "").replaceAll("\\s+$", "");
	}
	
	protected void setupCommandMaps() {
		mapCommand("truncate", HOST_TYPE_OSX, "gtruncate");
		mapCommand("stat", HOST_TYPE_OSX, "gstat");
	}
	
	protected void mapCommand(String canonical, int type, String osAlternative) {
		osCommandMap.putIfAbsent(canonical, new HashMap<Integer,String>());
		osCommandMap.get(canonical).put(type, osAlternative);
	}
	
	protected void parseUrl(String url) {
		String[] comps = url.split("@");
		if(comps.length == 1) user = null; // TODO: default to ssh config?
		else user = comps[0];
		
		comps = comps[comps.length-1].split(":");
		hostname = comps[0];
		remotePath = "";
		for(int i = 1; i < comps.length; i++) {
			if(i > 1) remotePath += ":";
			remotePath += comps[i];
		}
	}
	
	// TODO: all the stat stuff can get split off too
	
	protected Stat parseStat(String output) {
		Stat stat = new Stat();
		stat.setSize(parseStatSize(output));
		stat.setMtime(parseStatMtime(output));
		stat.setAtime(parseStatAtime(output));
		stat.setCtime(parseStatCtime(output));
		stat.setMode(parseStatMode(output));
		stat.setType(parseStatType(output));
		stat.setDevMajor(parseStatDeviceMajor(output));
		stat.setDevMinor(parseStatDeviceMinor(output));
		stat.setInodeId(parseStatInodeId(output));
		stat.setUid(parseStatUid(output));
		stat.setUser(parseStatUser(output));
		stat.setGid(parseStatGid(output));
		stat.setGroup(parseStatGroup(output));
		return stat;
	}
	
	private long parseStatSize(String output) {
		return Integer.parseInt(patternFromString("\\s*Size: (\\d+)", output));
	}

	private long parseStatMtime(String output) {
		String regex = "\\s*Modify: "+statDateRegex()+"$";
		return statDateToEpochNs(patternFromString(regex, output));
	}
	
	private long parseStatAtime(String output) {
		String regex = "\\s*Access: "+statDateRegex()+"$";
		return statDateToEpochNs(patternFromString(regex, output));
	}
	
	private long parseStatCtime(String output) {
		String regex = "\\s*Change: "+statDateRegex()+"$";
		return statDateToEpochNs(patternFromString(regex, output));
	}
	
	private int parseStatMode(String output) {
		return Integer.parseInt(patternFromString("Access: \\((0\\d+)/", output), 8);
	}
	
	private int parseStatType(String output) {
		String type = patternFromString("\\s*ID Block: \\d+\\s+([\\w ]*\\w)", output);
		if(type == null) throw new RuntimeException("Unable to locate device type in stat");
		if(type.equals("regular file")) return Stat.TYPE_REGULAR_FILE;
		if(type.equals("directory")) return Stat.TYPE_DIRECTORY;
		if(type.equals("symbolic link")) return Stat.TYPE_SYMLINK;
		if(type.equals("character special device")) return Stat.TYPE_CHARACTER_DEVICE;
		if(type.equals("block special device")) return Stat.TYPE_BLOCK_DEVICE;
		if(type.equals("fifo")) return Stat.TYPE_FIFO;
		throw new RuntimeException("Unrecognized stat type " + type);
	}
	
	private int parseStatDeviceMajor(String output) {
		return Integer.parseInt(patternFromString("\\s*Device type: (\\d+),\\d+", output));
	}
	
	private int parseStatDeviceMinor(String output) {
		return Integer.parseInt(patternFromString("\\s*Device type: \\d+,(\\d+)", output));
	}
	
	private int parseStatInodeId(String output) {
		return Integer.parseInt(patternFromString("\\s*Inode: (\\d+)", output));
	}
	
	private int parseStatUid(String output) {
		return Integer.parseInt(patternFromString("\\s*Uid: \\(\\s*(\\d+)\\s*/", output));
	}
	
	private String parseStatUser(String output) {
		return patternFromString("\\s*Uid: \\(\\s*\\d+\\s*/\\s*(\\w+)\\s*\\)", output);
	}
	
	private int parseStatGid(String output) {
		return Integer.parseInt(patternFromString("\\s*Gid: \\(\\s*(\\d+)\\s*/", output));
	}
	
	private String parseStatGroup(String output) {
		return patternFromString("\\s*Gid: \\(\\s*\\d+\\s*/\\s*(\\w+)\\s*\\)", output);
	}
	
	private String patternFromString(String regex, String data) {
		Matcher matcher = Pattern.compile(regex).matcher(data);
		if(!matcher.find()) return null;
		return matcher.group(1);
	}
	
	private long statDateToEpochNs(String statDate) {
		String pattern = "(\\d+)-(\\d+)-(\\d+) (\\d+):(\\d+):(\\d+)\\.(\\d+) ([-+]\\d+)";
		Matcher matcher = Pattern.compile(pattern).matcher(statDate);
		if(!matcher.find()) throw new RuntimeException("Unable to parse stat date " + statDate);
		int year = Integer.parseInt(matcher.group(1)),
			month = Integer.parseInt(matcher.group(2)),
			day = Integer.parseInt(matcher.group(3)),
			hour = Integer.parseInt(matcher.group(4)),
			minute = Integer.parseInt(matcher.group(5)),
			second = Integer.parseInt(matcher.group(6)),
			nanosecond = Integer.parseInt(matcher.group(7)),
			timezone = Integer.parseInt(matcher.group(8));
		try {
			String munged = String.format("%04d %02d %02d %02d %02d %02d %s",
					year, month, day, hour, minute, second, timezone);
			long ms = (new SimpleDateFormat("yyyy MM dd HH mm ss Z")).parse(munged).getTime();
			return 1000l*1000l*ms+nanosecond;
		} catch (ParseException e) {
			throw new RuntimeException("Unable to parse stat date " + statDate);
		}
	}
	
	private String statDateRegex() {
		return "(\\d+-\\d+-\\d+ \\d+:\\d+:\\d+\\.\\d+ [-+]\\d+)";
	}
}
