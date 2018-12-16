package com.acrescrypto.zksync.fs.zkfs.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;

public class SubscriptionServiceTest {
	SubscriptionService sub;
	
	@Before
	public void beforeEach() {
		sub = new SubscriptionService();
	}
	
	JsonObject makeTestJson() {
		return Json.createObjectBuilder()
			.add("bool.true", true)
			.add("bool.false", false)
			.add("int", 1234)
			.add("long", 8589934592L)
			.add("double", 3.14156)
			.add("string", "test string")
			.add("null", JsonValue.NULL)
			.build();
	}
	
	@Test
	public void testMakesBooleanCallbacksWithBoolValueWhenFalse() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("bool.false").asBoolean((v)->{
			hitCallback.setTrue();
			assertFalse(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesBooleanCallbacksWithBoolValueWhenTrue() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("bool.true").asBoolean((v)->{
			hitCallback.setTrue();
			assertTrue(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesBooleanCallbacksWithDefaultValueWhenNull() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asBoolean(true, (v)->{
			hitCallback.setTrue();
			assertTrue(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesBooleanCallbacksWithDefaultValueWhenWrongType() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("string").asBoolean(true, (v)->{
			hitCallback.setTrue();
			assertTrue(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesBooleanCallbacksWithNullDefaultIfNoDefaultGiven() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asBoolean((v)->{
			hitCallback.setTrue();
			assertNull(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	
	
	@Test
	public void testMakesIntCallbacksWithIntValue() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("int").asInt((v)->{
			hitCallback.setTrue();
			assertEquals(makeTestJson().getInt("int"), v.intValue());
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}

	@Test
	public void testMakesIntCallbacksWithDefaultWhenNull() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asInt(4321, (v)->{
			hitCallback.setTrue();
			assertEquals(4321, v.intValue());
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesIntCallbacksWithDefaultWhenWrongType() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("string").asInt(4321, (v)->{
			hitCallback.setTrue();
			assertEquals(4321, v.intValue());
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesIntCallbacksWithNullDefaultIfNoDefaultGiven() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asInt((v)->{
			hitCallback.setTrue();
			assertNull(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	

	
	@Test
	public void testMakesLongCallbacksWithLongValue() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("long").asLong((v)->{
			hitCallback.setTrue();
			assertEquals(makeTestJson().getJsonNumber("long").longValue(), v.longValue());
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}

	@Test
	public void testMakesLongCallbacksWithDefaultWhenNull() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asLong(4321, (v)->{
			hitCallback.setTrue();
			assertEquals(4321, v.intValue());
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesLongCallbacksWithDefaultWhenWrongType() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("string").asLong(4321, (v)->{
			hitCallback.setTrue();
			assertEquals(4321, v.intValue());
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesLongCallbacksWithNullDefaultIfNoDefaultGiven() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asLong((v)->{
			hitCallback.setTrue();
			assertNull(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}


	
	@Test
	public void testMakesDoubleCallbacksWithDoubleValue() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("double").asDouble((v)->{
			hitCallback.setTrue();
			assertEquals(makeTestJson().getJsonNumber("double").doubleValue(), v.doubleValue(), 1e-6);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}

	@Test
	public void testMakesDoubleCallbacksWithDefaultWhenNull() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asDouble(1.234, (v)->{
			hitCallback.setTrue();
			assertEquals(1.234, v.doubleValue(), 1e-6);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesDoubleCallbacksWithDefaultWhenWrongType() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("string").asDouble(1.234, (v)->{
			hitCallback.setTrue();
			assertEquals(1.234, v.doubleValue(), 1e-6);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesDoubleCallbacksWithNullDefaultIfNoDefaultGiven() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asDouble((v)->{
			hitCallback.setTrue();
			assertNull(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}	

	
	
	@Test
	public void testMakesStringCallbacksWithStringValue() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("string").asString((v)->{
			hitCallback.setTrue();
			assertEquals(makeTestJson().getString("string"), v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}

	@Test
	public void testMakesStringCallbacksWithDefaultWhenNull() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asString("foo", (v)->{
			hitCallback.setTrue();
			assertEquals("foo", v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesStringCallbacksWithDefaultWhenWrongType() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("double").asString("foo", (v)->{
			hitCallback.setTrue();
			assertEquals("foo", v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testMakesStringCallbacksWithNullDefaultIfNoDefaultGiven() {
		MutableBoolean hitCallback = new MutableBoolean();
		sub.subscribe("null").asString((v)->{
			hitCallback.setTrue();
			assertNull(v);
		});
		
		sub.updatedConfig(makeTestJson());
		assertTrue(hitCallback.booleanValue());
	}
	
	@Test
	public void testStopsSendingCallbacksWhenTokenClosed() {
		sub.subscribe("string").asString((v)->fail()).close();
		sub.updatedConfig(makeTestJson());
	}
	
	@Test
	public void testCloseDoesNotRemoveOtherCallback() {
		MutableInt hits = new MutableInt();
		sub.subscribe("string").asInt((v)->hits.increment());
		SubscriptionToken<?> token = sub.subscribe("string").asString((v)->fail());
		sub.subscribe("string").asString((v)->hits.increment());
		
		token.close();

		sub.updatedConfig(makeTestJson());
		assertEquals(2, hits.intValue());
	}
}
