package com.acrescrypto.zksync.fs.zkfs.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.zkfs.config.SubscriptionService.SubscriptionToken;

public class SubscriptionServiceTest {
	SubscriptionService sub;
	
	@BeforeClass
	public static void beforeAll() {
		TestUtils.startDebugMode();
	}
	
	@Before
	public void beforeEach() {
		sub = new SubscriptionService();
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
		TestUtils.stopDebugMode();
	}
	
	JsonObject makeTestJson() {
		return Json.createObjectBuilder()
			.performLookup("bool.true", true)
			.performLookup("bool.false", false)
			.performLookup("int", 1234)
			.performLookup("long", 8589934592L)
			.performLookup("double", 3.14156)
			.performLookup("string", "test string")
			.performLookup("null", JsonValue.NULL)
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
	public void testStopsSendingCallbacksWhenTokenClosed() {
		sub.subscribe("string").asString((v)->fail()).close();
		sub.updatedConfig(makeTestJson());
	}
	
	@Test
	public void testCloseDoesNotRemoveOtherCallback() {
		MutableInt hits = new MutableInt();
		sub.subscribe("string").asString((v)->hits.increment());
		SubscriptionToken<?> token = sub.subscribe("string").asString((v)->fail());
		sub.subscribe("string").asString((v)->hits.increment());
		
		token.close();

		sub.updatedConfig(makeTestJson());
		assertEquals(2, hits.intValue());
	}
}
