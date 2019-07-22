package com.acrescrypto.zksync.fs.zkfs.config;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.json.Json;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.acrescrypto.zksync.exceptions.ENOENTException;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionBuilder;

/* Store information specific to this user's copy of the archive; not shared with peers. Encrypted, though peers
 * would have knowledge of key if they did possess encrypted LocalConfig file.
 */
public class ConfigFile {
	protected SubscriptionService sub = new SubscriptionService();
	protected FS storage;
	protected ConcurrentHashMap<String,JsonValue> info = new ConcurrentHashMap<>();
	protected String path;
	protected HashMap<String,Object> defaults = new HashMap<>();
	
	protected Logger logger = LoggerFactory.getLogger(ConfigFile.class);
	protected boolean autowriteEnabled = true;
	
	public ConfigFile(FS storage, String path) throws IOException {
		this.storage = storage;
		this.path = path;
		
		try {
			read();
			logger.info("Config: Loaded existing file");
		} catch(ENOENTException exc) {
			logger.info("Config: No pre-existing config file");
		}
	}
	
	public String path() {
		return path;
	}
	
	protected void deserialize(byte[] serialized) {
		info.clear();
		JsonReader reader = Json.createReader(new StringReader(new String(serialized)));
		JsonObject json = reader.readObject();
		
		boolean oldAutowriteEnabled = autowriteEnabled;
		this.autowriteEnabled = false; // don't rewrite config when calling set()
		
		json.asJsonObject().entrySet().forEach((m)->{
			this.set(m.getKey(), m.getValue());
		});
		
		this.autowriteEnabled = oldAutowriteEnabled;
		
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
	
	protected synchronized void writeQuietly() {
		try {
			write();
		} catch(IOException exc) {
			logger.error("Caught exception logging " + path, exc);
		}
	}
	
	
	public SubscriptionBuilder subscribe(String key) {
		return sub.subscribe(key);
	}
	
	public synchronized void set(String key, JsonValue value) {
		logger.info("Config: Setting " + key + " -> " + value);
		info.put(key, value);
		sub.updatedKey(key, value);
		writeQuietly();
	}
	
	public void set(String key, boolean value) {
		JsonValue jvalue = value ? JsonValue.TRUE : JsonValue.FALSE;
		try { if(info.get(key).equals(jvalue)) return; } catch(NullPointerException exc) {}
		
		logger.info("Config: Setting " + key + " -> " + value);
		info.put(key, jvalue);
		sub.updatedKey(key, jvalue);
		writeQuietly();
	}
	
	public void set(String key, int value) {
		// For some reason, Json.createValue is throwing UnsupportedOperationExceptions... so here's a hack.
		
		try { if(((JsonNumber) info.get(key)).intValue() == value) return; } catch(NullPointerException exc) {}
		
		logger.info("Config: Setting " + key + " -> " + value);
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonNumber("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, jsonValue);
		writeQuietly();
	}
	
	public void set(String key, long value) {
		try { if(((JsonNumber) info.get(key)).longValue() == value) return; } catch(NullPointerException exc) {}
		
		logger.info("Config: Setting " + key + " -> " + value);
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonNumber("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, jsonValue);
		writeQuietly();
	}
	
	public void set(String key, double value) {
		try { if(((JsonNumber) info.get(key)).doubleValue() == value) return; } catch(NullPointerException exc) {}
		
		logger.info("Config: Setting " + key + " -> " + value);
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonNumber("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, jsonValue);
		writeQuietly();
	}
	
	public void set(String key, String value) {
		try { if(((JsonString) info.get(key)).getString() == value) return; } catch(NullPointerException exc) {}
		
		logger.info("Config: Setting " + key + " -> " + value);
		JsonValue jsonValue = Json.createObjectBuilder().add("x", value).build().getJsonString("x");
		info.put(key, jsonValue);
		sub.updatedKey(key, jsonValue);
		writeQuietly();
	}
	
	public void setDefault(String key, Object value) {
		if(hasKey(key)) return;
		defaults.put(key, value);
	}
	
	public boolean hasKey(String key) {
		return info.containsKey(key);
	}
	
	public Object get(String key) {
		JsonValue value = info.get(key);
		if(value.getValueType().equals(ValueType.FALSE)) return false;
		if(value.getValueType().equals(ValueType.TRUE)) return true;
		if(value.getValueType().equals(ValueType.NUMBER)) {
			double dvalue = getDouble(key);
			if(Math.floor(dvalue) != dvalue) return dvalue;
			
			long lvalue = getLong(key);
			if(Integer.MIN_VALUE <= lvalue && lvalue <= Integer.MAX_VALUE) {
				return getInt(key);
			} else {
				return lvalue;
			}
		}
		if(value.getValueType().equals(ValueType.STRING)) return getString(key);
		return null;
	}

	public boolean getBool(String key) {
		if(!hasKey(key)) return (boolean) defaults.get(key);
		return info.get(key).equals(JsonValue.TRUE);
	}
	
	public int getInt(String key) {
		if(!hasKey(key)) return safeInt(defaults.get(key));
		return ((JsonNumber) info.get(key)).intValue();
	}
	
	public long getLong(String key) {
		if(!hasKey(key)) return safeLong(defaults.get(key));
		return ((JsonNumber) info.get(key)).longValue();
	}
	
	public double getDouble(String key) {
		if(!hasKey(key)) return safeDouble(defaults.get(key));
		return ((JsonNumber) info.get(key)).doubleValue();
	}
	
	public String getString(String key) {
		if(!hasKey(key)) return (String) defaults.get(key);
		return ((JsonString) info.get(key)).getString();
	}
	
	protected Long safeLong(Object n) {
		if(n == null) return 0L;
		if(n instanceof Number) return ((Number) n).longValue();
		return null;
	}
	
	protected Integer safeInt(Object n) {
		if(n == null) return 0;
		if(n instanceof Number) return ((Number) n).intValue();
		return null;
	}
	
	protected Double safeDouble(Object n) {
		if(n == null) return 0.0;
		if(n instanceof Number) return ((Number) n).doubleValue();
		return null;
	}
	
	public Set<String> keys() {
		HashSet<String> keys = new HashSet<>();
		keys.addAll(defaults.keySet());
		keys.addAll(info.keySet());
		return keys;
	}
	
	public HashMap<String, Object> asHash() {
		HashMap<String,Object> r = new LinkedHashMap<>();
		ArrayList<String> sorted = new ArrayList<>(keys());
		sorted.sort(null);
		
		for(String key : sorted) {
			Object o = null;
			
			JsonValue v = info.get(key);
			if(v != null) {
				switch(v.getValueType()) {
				case STRING:
					o = new String(((JsonString) v).getString());
					break;
				case NUMBER:
					if(((JsonNumber) v).isIntegral()) {
						o = Long.valueOf(((JsonNumber) v).longValue());
					} else {
						o = Double.valueOf(((JsonNumber) v).doubleValue());
					}
					break;
				case TRUE:
					o = Boolean.TRUE;
					break;
				case FALSE:
					o = Boolean.FALSE;
					break;
				default:
					break;
				}
			} else {
				o = defaults.get(key);
			}
			
			r.put(key, o);
		}
		
		return r;
	}
	
	public SubscriptionService getSubsciptionService() {
		return sub;
	}
}
