package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionBuilder;

/* Store information specific to this user's copy of the archive; not shared with peers. Encrypted, though peers
 * would have knowledge of key if they did possess encrypted LocalConfig file.
 */
public class ConfigFile {
	/* want:
	 * net.blacklist.duration
	 * 
	 * net.swarm.port
	 * net.swarm.enabled
	 * net.swarm.defaultmaxpeers
	 * 
	 * net.dht.port
	 * net.dht.enabled
	 * 
	 * net.upnp.enabled
	 * 
	 * api.http.port
	 * 
	 * and an observer system:
	 * handle = config.subscribe("net.swarm.port").withInt((port)->rebind(port), 0);
	 * handle.close() // when owner is ready to relinquish, for memory management
	 * config.get("net.swarm.port").asInt(0)
	 */
	protected SubscriptionService sub = new SubscriptionService();
	protected FS storage;
	protected HashMap<String,JsonValue> info = new HashMap<>();
	protected String path;
	
	protected Logger logger = LoggerFactory.getLogger(ConfigFile.class);
	
	public ConfigFile(FS storage, String path) throws IOException {
		this.storage = storage;
		this.path = path;
		
		try {
			read();
		} catch(ENOENTException exc) {
		}
	}
	
	public String path() {
		return path;
	}
	
	protected void deserialize(byte[] serialized) {
		info.clear();
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		sub.updatedConfig(json);
	}
	
	protected byte[] serialize() {
		JsonObjectBuilder builder = Json.createObjectBuilder();
		for(String key : info.keySet()) {
			builder.add(key, info.get(key));
		}
		
		return builder.build().toString().getBytes();
	}
	
	protected void read() throws IOException {
		deserialize(storage.read(path()));
	}
	
	protected void write() throws IOException {
		storage.write(path(), serialize());
	}
	
	protected void writeQuietly() {
		try {
			storage.write(path(), serialize());
		} catch(IOException exc) {
			logger.error("Caught exception logging " + path, exc);
		}
	}
	
	
	public SubscriptionBuilder subscribe(String key) {
		return sub.subscribe(key);
	}
	
	public void set(String key, JsonValue value) {
		info.put(key, value);
		sub.updatedKey(key, value);
		writeQuietly();
	}
	
	public void set(String key, boolean value) {
		info.put(key, value ? JsonValue.TRUE : JsonValue.FALSE);
		sub.updatedKey(key, info.get(key));
		writeQuietly();
	}
	
	public void set(String key, int value) {
		// For some reason, Json.createValue is throwing UnsupportedOperationExceptions... so here's a hack.
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonNumber("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, info.get(key));
		writeQuietly();
	}
	
	public void set(String key, long value) {
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonNumber("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, info.get(key));
		writeQuietly();
	}
	
	public void set(String key, double value) {
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonNumber("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, info.get(key));
		writeQuietly();
	}
	
	public void set(String key, String value) {
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonNumber("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, info.get(key));
		writeQuietly();
	}
	

	public boolean hasKey(String key) {
		return info.containsKey(key);
	}
	

	public boolean getBool(String key) {
		return info.get(key).equals(JsonValue.TRUE);
	}
	
	public boolean getBool(String key, boolean defaultValue) {
		if(!hasKey(key)) return defaultValue;
		return info.get(key).equals(JsonValue.TRUE);
	}

	
	public int getInt(String key) {
		return ((JsonNumber) info.get(key)).intValue();
	}
	
	public int getInt(String key, int defaultValue) {
		if(!hasKey(key)) return defaultValue;
		return ((JsonNumber) info.get(key)).intValue();
	}
	

	public long getLong(String key) {
		return ((JsonNumber) info.get(key)).longValue();
	}
	
	public long getLong(String key, long defaultValue) {
		if(!hasKey(key)) return defaultValue;
		return ((JsonNumber) info.get(key)).longValue();
	}

	
	public double getDouble(String key) {
		return ((JsonNumber) info.get(key)).doubleValue();
	}
	
	public double getDouble(String key, double defaultValue) {
		if(!hasKey(key)) return defaultValue;
		return ((JsonNumber) info.get(key)).doubleValue();
	}

	
	public String getString(String key) {
		return ((JsonString) info.get(key)).getString();
	}
	
	public String getString(String key, String defaultValue) {
		if(!hasKey(key)) return defaultValue;
		return ((JsonString) info.get(key)).getString();
	}

	public HashMap<String, Object> asHash() {
		HashMap<String,Object> r = new HashMap<>();
		for(String key : info.keySet()) {
			Object o = null;
			
			JsonValue v = info.get(key);
			switch(v.getValueType()) {
			case STRING:
				o = new String(((JsonString) v).getString());
				break;
			case NUMBER:
				if(((JsonNumber) v).isIntegral()) {
					o = new Long(((JsonNumber) v).longValue());
				} else {
					o = new Double(((JsonNumber) v).doubleValue());
				}
				break;
			case TRUE:
				o = new Boolean(true);
				break;
			case FALSE:
				o = new Boolean(false);
				break;
			default:
				break;
			}
			
			r.put(key, o);
		}
		
		return r;
	}
}
