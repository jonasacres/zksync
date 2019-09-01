package com.acrescrypto.zksyncweb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.logging.ConsoleHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.acrescrypto.zksync.fs.Stat;
import com.acrescrypto.zksync.fs.zkfs.PageTree;
import com.acrescrypto.zksync.fs.zkfs.PageTree.PageTreeStats;
import com.acrescrypto.zksync.fs.zkfs.RevisionInfo;
import com.acrescrypto.zksync.fs.zkfs.RevisionTag;
import com.acrescrypto.zksync.fs.zkfs.ZKArchive;
import com.acrescrypto.zksync.fs.zkfs.ZKArchiveConfig;
import com.acrescrypto.zksync.fs.zkfs.ZKFS;
import com.acrescrypto.zksync.utility.BandwidthMonitor;
import com.acrescrypto.zksync.utility.Util;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WebTestUtils {
	public static void squelchGrizzlyLogs() {
		String[] logClasses = {
				"org.glassfish.grizzly.http.server.NetworkListener",
				"org.glassfish.grizzly.http.server.HttpServer",
		};

		for(String logClass : logClasses) {
			Logger l = Logger.getLogger(logClass);
			l.setLevel(Level.OFF);
			l.setUseParentHandlers(false);
			for(Handler handler : l.getHandlers()) {
				handler.setLevel(Level.OFF);
			}
			ConsoleHandler ch = new ConsoleHandler();
			ch.setLevel(Level.OFF);
			l.addHandler(ch);
		}
	}

	public static WebTarget buildTarget(WebTarget target, String path) {
		URL url;
		while(path.startsWith("/")) path = path.substring(1);
		
		try {
			url = new URL("http://example.com/" + path);
		} catch(MalformedURLException exc) {
			throw new RuntimeException(exc);
		}

		String query = url.getQuery();
		target = target.path(url.getPath());
		try {
			if(query != null) {
				String[] pairs = query.split("&");
				for (String pair : pairs) {
					int idx = pair.indexOf("=");
					String key = URLDecoder.decode(pair.substring(0, idx), "UTF-8");
					String value = URLDecoder.decode(pair.substring(idx + 1), "UTF-8");
					target = target.queryParam(key, value);
				}
			}
		} catch(UnsupportedEncodingException exc) {
			throw new RuntimeException(exc);
		}

		return target;
	}

	public static JsonNode requestGet(WebTarget target, String path) {
		return handleResponse(buildTarget(target, path).request().get(String.class));
	}

	public static byte[] requestBinaryGet(WebTarget target, String path) {
		return buildTarget(target, path).request().get(byte[].class);
	}

	public static JsonNode requestGetWithError(WebTarget target, int expectedStatus, String path) {
		try {
			return handleResponseWithError(expectedStatus, buildTarget(target, path).request().get(String.class));
		} catch(WebApplicationException exc) {
			String json = exc.getResponse().readEntity(String.class);
			assertEquals(expectedStatus, exc.getResponse().getStatus());
			return handleResponseWithError(expectedStatus, json);
		}
	}

	public static JsonNode requestBinaryPost(WebTarget target, String path, byte[] data) {
		Entity<Object> entity = Entity.entity(data, MediaType.APPLICATION_OCTET_STREAM);
		return handleResponse(buildTarget(target, path).request().post(entity, String.class));
	}

	public static JsonNode requestBinaryPostWithError(WebTarget target, int expectedError, String path, byte[] data) {
		Entity<Object> entity = Entity.entity(data, MediaType.APPLICATION_OCTET_STREAM);
		try {
			return handleResponseWithError(expectedError, buildTarget(target, path).request().post(entity, String.class));
		} catch(WebApplicationException exc) {
			String json = exc.getResponse().readEntity(String.class);
			assertEquals(expectedError, exc.getResponse().getStatus());
			return handleResponseWithError(expectedError, json);
		}
	}

	public static JsonNode requestPost(WebTarget target, String path, Object data) {
		Entity<Object> entity = Entity.entity(data, MediaType.APPLICATION_JSON_TYPE);
		return handleResponse(buildTarget(target, path).request().post(entity, String.class));
	}

	public static JsonNode requestPostWithError(WebTarget target, int expectedError, String path, Object data) {
		Entity<Object> entity = Entity.entity(data, MediaType.APPLICATION_JSON_TYPE);
		try {
			return handleResponseWithError(expectedError, buildTarget(target, path).request().post(entity, String.class));
		} catch(WebApplicationException exc) {
			String json = exc.getResponse().readEntity(String.class);
			assertEquals(expectedError, exc.getResponse().getStatus());
			return handleResponseWithError(expectedError, json);
		}
	}

	public static JsonNode requestPut(WebTarget target, String path, Object data) {
		Entity<Object> entity = Entity.entity(data, MediaType.APPLICATION_JSON_TYPE);
		return handleResponse(buildTarget(target, path).request().put(entity, String.class));
	}

	public static JsonNode requestPutWithError(WebTarget target, int expectedError, String path, Object data) {
		Entity<Object> entity = Entity.entity(data, MediaType.APPLICATION_JSON_TYPE);
		try {
			return handleResponseWithError(expectedError, buildTarget(target, path).request().put(entity, String.class));
		} catch(WebApplicationException exc) {
			String json = exc.getResponse().readEntity(String.class);
			assertEquals(expectedError, exc.getResponse().getStatus());
			return handleResponseWithError(expectedError, json);
		}
	}

	public static JsonNode requestDelete(WebTarget target, String path) {
		return handleResponse(buildTarget(target, path).request().delete(String.class));
	}

	public static JsonNode requestDeleteWithError(WebTarget target, int expectedStatus, String path) {
		try {
			return handleResponseWithError(expectedStatus, buildTarget(target, path).request().delete(String.class));
		} catch(WebApplicationException exc) {
			String json = exc.getResponse().readEntity(String.class);
			assertEquals(expectedStatus, exc.getResponse().getStatus());
			return handleResponseWithError(expectedStatus, json);
		}
	}

	public static JsonNode handleResponse(String responseMsg) {
		ObjectMapper mapper = new ObjectMapperProvider().getContext(null);
		JsonNode json = null;
		try {
			json = mapper.readTree(responseMsg);
		} catch (IOException exc) {
			exc.printStackTrace();
			fail("parser error");
		}

		checkFields(json);
		assertEquals(2, json.get("status").asInt()/100);
		assertFalse(json.hasNonNull("errmsg"));

		return json.get("resp");
	}

	public static JsonNode handleResponseWithError(int expectedStatus, String responseMsg) {
		ObjectMapper mapper = new ObjectMapperProvider().getContext(null);
		JsonNode json = null;
		try {
			json = mapper.readTree(responseMsg);
		} catch (IOException exc) {
			exc.printStackTrace();
			fail("parser error");
		}

		checkFields(json);
		assertEquals(expectedStatus, json.get("status").asInt());
		assertTrue(json.hasNonNull("errmsg"));

		return json;
	}

	public static void checkFields(JsonNode json) {
		class MutableInt {
			int value;

			public void increment() { value++; }
			public int intValue() { return value; }
		}
		MutableInt fieldCount = new MutableInt();
		ArrayList<String> legalNames = new ArrayList<>();
		legalNames.add("resp");
		legalNames.add("errmsg");
		legalNames.add("status");

		json.fieldNames().forEachRemaining((name)->{
			if(!legalNames.contains(name)) fail("Unexpected field " + name);
			fieldCount.increment();
		});

		assertEquals(3, fieldCount.intValue());
	}

	public static String transformRevTag(RevisionTag tag) {
		String base64 = Base64.getEncoder().encodeToString(tag.getBytes());
		return Util.toWebSafeBase64(base64);
	}

	public static String transformArchiveId(ZKArchive archive) {
		String base64 = Base64.getEncoder().encodeToString(archive.getConfig().getArchiveId());
		return Util.toWebSafeBase64(base64);
	}

	public static void validatePathStat(ZKFS fs, String prefix, JsonNode pathStat) throws IOException {
		String fullPath = Paths.get(prefix, pathStat.get("path").asText()).toString();

		Stat stat = null;
		JsonNode statNode = pathStat.get("stat");

		try {
			stat = fs.stat(fullPath);
		} catch (IOException exc) {
			fail();
		}

		PageTreeStats treeStats = new PageTree(fs.inodeForPath(fullPath)).getStats();
		assertEquals(1.0, pathStat.get("fractionAcquired").asDouble(), 1e-5);
		assertEquals(treeStats.totalPages, pathStat.get("numPages").asLong());
		assertEquals(treeStats.totalChunks, pathStat.get("numChunks").asLong());
		assertEquals(treeStats.numCachedPages, pathStat.get("numPagesAcquired").asLong());
		assertEquals(treeStats.numCachedChunks, pathStat.get("numChunksAcquired").asLong());

		assertEquals(stat.getSize(), statNode.get("size").asLong());
		assertEquals(stat.getAtime(), statNode.get("atime").asLong());
		assertEquals(stat.getMtime(), statNode.get("mtime").asLong());
		assertEquals(stat.getCtime(), statNode.get("ctime").asLong());
		assertEquals(stat.getInodeId(), statNode.get("inodeId").asLong());
		assertEquals(stat.getGid(), statNode.get("gid").asInt());
		assertEquals(stat.getGroup(), statNode.get("group").asText());
		assertEquals(stat.getUid(), statNode.get("uid").asInt());
		assertEquals(stat.getUser(), statNode.get("user").asText());
		assertEquals(stat.getDevMajor(), statNode.get("devMajor").asInt());
		assertEquals(stat.getDevMinor(), statNode.get("devMinor").asInt());
		assertEquals(stat.getMode(), statNode.get("mode").asInt());
		assertEquals(stat.getType(), statNode.get("type").asInt());
	}

	public static void validateRevisionInfo(ZKArchiveConfig config, JsonNode resp) throws IOException {
		RevisionTag tag = new RevisionTag(config, resp.get("revTag").binaryValue(), false);
		RevisionInfo info = tag.getInfo();

		assertEquals(info.getGeneration(), resp.get("generation").longValue());
		assertEquals(info.getNumParents(), resp.get("parents").size());
		ArrayList<RevisionTag> tags = new ArrayList<>(info.getParents());
		resp.get("parents").forEach((parent)->{
			assertTrue(tags.removeIf((t)->{
				try {
					return Arrays.equals(t.getBytes(), parent.get("revTag").binaryValue());
				} catch (IOException e) {
					return false;
				}
			}));
		});

		assertEquals(0, tags.size());
	}

	public static void rigMonitors(BandwidthMonitor[] monitors, int targetBytesPerSecond) {
		int expMs = monitors[0].getSampleExpirationMs();
		double expSecs = expMs / 1000.0;
		long bytesPerInterval = (long) (targetBytesPerSecond * expSecs);

		Util.setCurrentTimeMillis(Util.currentTimeMillis() + expMs - 1);

		for(BandwidthMonitor monitor : monitors) {
			monitor.observeTraffic(bytesPerInterval);
		}

		Util.setCurrentTimeMillis(Util.currentTimeMillis() + expMs - 1);
	}
}
