package com.acrescrypto.zksync.fs.zkfs.config;

import java.util.HashMap;
import java.util.LinkedList;

import javax.json.JsonNumber;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionService {
	class SubscriptionToken<T> implements AutoCloseable {
		String key;
		SubscriptionCallback<T> callback;
		T defaultValue;
		Class<?> type;
		
		public SubscriptionToken(Class<?> type, String key, T defaultValue, SubscriptionCallback<T> callback) {
			this.type = type;
			this.key = key;
			this.defaultValue = defaultValue;
			this.callback = callback;
			addToken(this);
		}
		
		@SuppressWarnings("unchecked")
		public void update(Object value) {
			callback.callback((T) value);
		}
		
		public void close() {
			removeToken(this);
		}
	}
	
	class SubscriptionBuilder {
		String key;
		
		public SubscriptionBuilder(String key) {
			this.key = key;
		}
		
		
		SubscriptionToken<Boolean> asBoolean(SubscriptionCallback<Boolean> callback) {
			return new SubscriptionToken<Boolean>(Boolean.class, key, null, callback);
		}

		SubscriptionToken<Boolean> asBoolean(boolean defaultValue, SubscriptionCallback<Boolean> callback) {
			return new SubscriptionToken<Boolean>(Boolean.class, key, defaultValue, callback);
		}
		
		
		SubscriptionToken<Integer> asInt(SubscriptionCallback<Integer> callback) {
			return new SubscriptionToken<Integer>(Integer.class, key, null, callback);
		}

		SubscriptionToken<Integer> asInt(int defaultValue, SubscriptionCallback<Integer> callback) {
			return new SubscriptionToken<Integer>(Integer.class, key, defaultValue, callback);
		}
		
		
		SubscriptionToken<Long> asLong(SubscriptionCallback<Long> callback) {
			return new SubscriptionToken<Long>(Long.class, key, null, callback);
		}

		SubscriptionToken<Long> asLong(long defaultValue, SubscriptionCallback<Long> callback) {
			return new SubscriptionToken<Long>(Long.class, key, defaultValue, callback);
		}
		
		
		SubscriptionToken<Double> asDouble(SubscriptionCallback<Double> callback) {
			return new SubscriptionToken<Double>(Double.class, key, null, callback);
		}

		SubscriptionToken<Double> asDouble(double defaultValue, SubscriptionCallback<Double> callback) {
			return new SubscriptionToken<Double>(Double.class, key, defaultValue, callback);
		}
		
		
		SubscriptionToken<String> asString(SubscriptionCallback<String> callback) {
			return new SubscriptionToken<String>(String.class, key, null, callback);
		}

		SubscriptionToken<String> asString(String defaultValue, SubscriptionCallback<String> callback) {
			return new SubscriptionToken<String>(String.class, key, defaultValue, callback);
		}
	}
	
	public interface SubscriptionCallback<T> {
		void callback(T value);
	}
	
	protected Logger logger = LoggerFactory.getLogger(SubscriptionService.class);
	HashMap<String,HashMap<Class<?>, LinkedList<SubscriptionToken<?>>>> subscriptions = new HashMap<>();
	
	public SubscriptionService() {
	}
	
	public SubscriptionBuilder subscribe(String key) {
		return new SubscriptionBuilder(key);
	}
	
	public synchronized void updatedConfig(JsonValue top) {
		top.asJsonObject().entrySet().forEach((m)->{
			updatedKey(m.getKey(), m.getValue());
		});
	}
	
	public synchronized void updatedKey(String key, JsonValue value) {
		HashMap<Class<?>, LinkedList<SubscriptionToken<?>>> keyMap = subscriptions.get(key);
		if(keyMap == null) return;
		
		for(Class<?> type : keyMap.keySet()) {
			for(SubscriptionToken<?> token : keyMap.get(type)) {
				try {
					updateToken(key, value, token);
				} catch(Exception exc) {
					logger.error("Caught exception when processing config key " + key + " as " + type, exc);
				}
			}
		}
	}
	
	protected synchronized void updateToken(String key, JsonValue value, SubscriptionToken<?> token) {
		Class<?> type = token.type;
		if(value.getValueType().equals(ValueType.NULL)) {
			token.update(token.defaultValue);
			return;
		}
		
		if(type.equals(Boolean.class)) {
			if(value.equals(JsonValue.TRUE) || value.equals(JsonValue.FALSE)) {
				token.update(value.equals(JsonValue.TRUE));
			} else {
				token.update(token.defaultValue);
			}
		} else if(type.equals(Integer.class)) {
			if((value instanceof JsonNumber)) {
				token.update(((JsonNumber) value).intValue());
			} else {
				token.update(token.defaultValue);
			}
		} else if(type.equals(Long.class)) {
			if((value instanceof JsonNumber)) {
				token.update(((JsonNumber) value).longValue());
			} else {
				token.update(token.defaultValue);
			}
		} else if(type.equals(Double.class)) {
			if((value instanceof JsonNumber)) {
				token.update(((JsonNumber) value).doubleValue());
			} else {
				token.update(token.defaultValue);
			}
		} else if(type.equals(String.class)) {
			if((value instanceof JsonString)) {
				token.update(((JsonString) value).getString());
			} else {
				token.update(token.defaultValue);
			}
		}
	}
	
	protected synchronized void removeToken(SubscriptionToken<?> token) {
		HashMap<Class<?>, LinkedList<SubscriptionToken<?>>> keyMap = subscriptions.get(token.key);
		if(keyMap == null) return;
		
		LinkedList<SubscriptionToken<?>> tokens = keyMap.get(token.type);
		if(tokens == null) return;
		
		tokens.remove(token);
		if(!tokens.isEmpty()) return;
		
		keyMap.remove(token.type);
		if(!keyMap.isEmpty()) return;
		
		subscriptions.remove(token.key);
	}
	
	protected synchronized void addToken(SubscriptionToken<?> token) {
		HashMap<Class<?>, LinkedList<SubscriptionToken<?>>> keyMap = subscriptions.get(token.key);
		if(keyMap == null) {
			keyMap = new HashMap<>();
			subscriptions.put(token.key, keyMap);
		}
		
		LinkedList<SubscriptionToken<?>> tokens = keyMap.get(token.type);
		if(tokens == null) {
			tokens = new LinkedList<>();
			keyMap.put(token.type, tokens);
		}
		
		tokens.add(token);
	}
}
