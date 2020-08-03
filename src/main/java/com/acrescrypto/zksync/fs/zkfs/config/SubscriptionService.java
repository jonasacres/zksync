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
	public class SubscriptionToken<T> implements AutoCloseable {
		String key;
		SubscriptionCallback<T> callback;
		Class<?> type;
		
		public SubscriptionToken(Class<?> type, String key, SubscriptionCallback<T> callback) {
			this.type = type;
			this.key = key;
			this.callback = callback;
			addToken(this);
		}
		
		@SuppressWarnings("unchecked")
		public void update(Object value) {
			callback.callback((T) value);
		}
		
		public void close() {
			removeToken(this);
			this.callback = null;
		}
	}
	
	public class SubscriptionBuilder {
		String key;
		
		public SubscriptionBuilder(String key) {
			this.key = key;
		}
		
		
		public SubscriptionToken<Boolean> asBoolean(SubscriptionCallback<Boolean> callback) {
			return new SubscriptionToken<Boolean>(Boolean.class, key, callback);
		}

		
		public SubscriptionToken<Integer> asInt(SubscriptionCallback<Integer> callback) {
			return new SubscriptionToken<Integer>(Integer.class, key, callback);
		}
		
		
		public SubscriptionToken<Long> asLong(SubscriptionCallback<Long> callback) {
			return new SubscriptionToken<Long>(Long.class, key, callback);
		}

		
		public SubscriptionToken<Double> asDouble(SubscriptionCallback<Double> callback) {
			return new SubscriptionToken<Double>(Double.class, key, callback);
		}

		public SubscriptionToken<String> asString(SubscriptionCallback<String> callback) {
			return new SubscriptionToken<String>(String.class, key, callback);
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
			LinkedList<SubscriptionToken<?>> tokens = new LinkedList<>(keyMap.get(type));
			for(SubscriptionToken<?> token : tokens) {
				try {
					updateToken(key, value, token);
				} catch(Exception exc) {
					logger.error("Caught exception when processing config key " + key + " as " + type, exc);
				}
			}
		}
	}
	
	protected synchronized <T> void updateToken(String key, JsonValue value, SubscriptionToken<T> token) {
		Object obj = decodeValue(value);
		if(obj != null && !token.type.isAssignableFrom(obj.getClass())) {
			if(!(Number.class.isAssignableFrom(token.type) && obj instanceof Number)) {
				token.update(null);
				return;
			}
			
			Number n = (Number) obj;
			if     (token.type.isAssignableFrom(Long   .class)) token.update(n.longValue());
			else if(token.type.isAssignableFrom(Integer.class)) token.update(n.intValue());
			else if(token.type.isAssignableFrom(Short  .class)) token.update(n.shortValue());
			else if(token.type.isAssignableFrom(Byte   .class)) token.update(n.byteValue());
			else if(token.type.isAssignableFrom(Double .class)) token.update(n.doubleValue());
			else if(token.type.isAssignableFrom(Float  .class)) token.update(n.floatValue());
			else token.update(null);
			return;
		}
		
		token.update(decodeValue(value));
	}
	
	protected Object decodeValue(JsonValue value) {
		if(value.getValueType().equals(ValueType.FALSE)) return false;
		if(value.getValueType().equals(ValueType.TRUE)) return true;
		if(value.getValueType().equals(ValueType.NUMBER)) {
			double dvalue = ((JsonNumber) value).doubleValue();
			if(Math.floor(dvalue) != dvalue) return dvalue;
			
			long lvalue = ((JsonNumber) value).longValue();
			if(Integer.MIN_VALUE <= lvalue && lvalue <= Integer.MAX_VALUE) {
				return ((JsonNumber) value).intValue();
			} else {
				return lvalue;
			}
		}
		if(value.getValueType().equals(ValueType.STRING)) return ((JsonString) value).getString();
		return null;
	}
	
	protected synchronized void removeToken(SubscriptionToken<?> token) {
		HashMap<Class<?>, LinkedList<SubscriptionToken<?>>> keyMap = subscriptions.get(token.key);
		if(keyMap != null) {
			LinkedList<SubscriptionToken<?>> tokens = keyMap.get(token.type);
			if(tokens != null) {
				tokens.remove(token);
				if(!tokens.isEmpty()) return;
			}

			keyMap.remove(token.type);
			
			if(!keyMap.isEmpty()) {
				return;
			}
		}
		
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
	
	public String dumpSubscriptions() {
		StringBuilder sb = new StringBuilder(String.format("%5d Subscriptions\n", subscriptions.size()));
		subscriptions.forEach((key, submap)->{
			sb.append(String.format("\t%5d %-30s\n", submap.size(), key));
			submap.forEach((cls, subs)->{
				sb.append(String.format("\t\t%5d %-30s\n", subs.size(), cls.getSimpleName()));
				subs.forEach((token)->{
					sb.append(String.format("\t\t\t%s\n", token.callback));
				});
			});
		});
		
		return sb.toString();
	}
}
