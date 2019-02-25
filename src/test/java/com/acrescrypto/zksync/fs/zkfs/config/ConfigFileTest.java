package com.acrescrypto.zksync.fs.zkfs.config;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.FS;
import com.acrescrypto.zksync.fs.ramfs.RAMFS;

public class ConfigFileTest {
	ConfigFile config;
	FS fs;
	
	@Before
	public void beforeEach() throws IOException {
		fs = new RAMFS();
		config = new ConfigFile(fs, "config");
	}
	
	@AfterClass
	public static void afterAll() {
		TestUtils.assertTidy();
	}
	
	@Test
	public void testSetBoolean() {
		config.set("boolean", true);
		assertTrue(config.getBool("boolean"));
	}
	
	@Test
	public void testSetInt() {
		config.set("int", 1234);
		assertEquals(1234, config.getInt("int"));
	}
	
	@Test
	public void testSetLong() {
		config.set("long", Long.MAX_VALUE);
		assertEquals(Long.MAX_VALUE, config.getLong("long"));
	}

	@Test
	public void testSetDouble() {
		config.set("double", Math.PI);
		assertEquals(Math.PI, config.getDouble("double"), 0);
	}
	
	@Test
	public void testSetString() {
		config.set("string", "hello world");
		assertEquals("hello world", config.getString("string"));
	}
	
	@Test
	public void testHasKeyReturnsFalseIfKeyNotSet() {
		assertFalse(config.hasKey("foo"));
	}
	
	@Test
	public void testHasKeyReturnsTrueIfKeySet() {
		config.set("foo", true);
		assertTrue(config.hasKey("foo"));
	}
	
	@Test
	public void testHasKeyReturnsFalseIfDefaultSetAndKeyNotSet() {
		config.setDefault("foo", true);
		assertFalse(config.hasKey("foo"));
	}
	
	@Test(expected=NullPointerException.class)
	public void testGetThrowsExceptionIfKeyNotSet() {
		config.getBool("nonexistent");
	}
	
	@Test
	public void testGetReturnsDefaultIfKeyNotSetAndDefaultSet() {
		config.setDefault("default", 1234);
		assertEquals(1234, config.getInt("default"));
	}
	
	@Test
	public void testSerialization() throws IOException {
		config.set("bool", true);
		config.set("int", 1234);
		config.set("long", Long.MAX_VALUE);
		config.set("double", Math.E);
		config.set("string", "hello world");
		
		ConfigFile config2 = new ConfigFile(fs, "config");
		assertEquals(config.getBool("bool"), config2.getBool("bool"));
		assertEquals(config.getInt("int"), config2.getInt("int"));
		assertEquals(config.getLong("long"), config2.getLong("long"));
		assertEquals(config.getDouble("double"), config2.getDouble("double"), 1e-8);
		assertEquals(config.getString("string"), config2.getString("string"));
	}
}
