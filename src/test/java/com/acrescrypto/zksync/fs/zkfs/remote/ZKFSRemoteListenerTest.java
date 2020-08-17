package com.acrescrypto.zksync.fs.zkfs.remote;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.State;

public class ZKFSRemoteListenerTest {
    public final static int TEST_PORT = ZKFSRemoteListener.DEFAULT_PORT + 1;
    ZKFSRemoteListener                listener;
    
    @BeforeClass
    public static void beforeAll() {
        TestUtils.startDebugMode();
    }
    
    @Before
    public void beforeEach() throws IOException {
        ConfigDefaults.getActiveDefaults().set("net.remotefs.enabled", true);
        ConfigDefaults.getActiveDefaults().set("net.remotefs.address", testAddress());
        ConfigDefaults.getActiveDefaults().set("net.remotefs.port",    testPort());
        
        State.setTestState();
        listener = new ZKFSRemoteListener(State.sharedState());
        
        assertTrue(Util.waitUntil(100, ()->listener.isListening()));
    }
    
    @After
    public void afterEach() throws IOException {
        listener.close();
        ConfigDefaults.resetDefaults();
        State.clearState();
    }
    
    public void setRemotefsEnabled(boolean enabled) throws IOException {
        State.sharedState    ()
             .getMaster      ()
             .getGlobalConfig()
             .set("net.remotefs.enabled", enabled);
        
        assertTrue(Util.waitUntil(100, ()->listener.isListening() == enabled));
    }
    
    public void setRemotefsAddress(String address) throws IOException {
        State.sharedState    ()
             .getMaster      ()
             .getGlobalConfig()
             .set("net.remotefs.address", address);
    }
    
    public void setRemotefsPort(int port) throws IOException {
        State.sharedState    ()
             .getMaster      ()
             .getGlobalConfig()
             .set("net.remotefs.port", port);
    }
    
    public String testAddress() {
        return "127.0.0.1";
    }
    
    public int testPort() {
        return TEST_PORT;
    }
    
    @AfterClass
    public static void afterAll() {
        TestUtils.stopDebugMode();
        TestUtils.assertTidy();
    }
    
    @Test
    public void testConstructorSetsState() throws IOException {
        assertEquals(State.sharedState(), listener.state());
    }
    
    @Test
    public void testConstructorDoesNotOpenSocketIfConfigNetRemotefsEnabledFalse() throws IOException {
        listener.close();
        setRemotefsEnabled(false);
        
        try(ZKFSRemoteListener newListener = new ZKFSRemoteListener(State.sharedState())) {
            assertFalse(newListener.isListening());
        }
    }
    
    @Test
    public void testConstructorOpensSocketIfConfigNetRemotefsEnabledTrue() {
        assertTrue(listener.isListening());
    }
    
    @Test
    public void testBindsToRequestedAddress() throws IOException {
        assertEquals(State   .sharedState()
                             .getMaster()
                             .getGlobalConfig()
                             .get("net.remotefs.address"),
                     listener.listenSocket
                             .socket()
                             .getInetAddress()
                             .getHostAddress());
    }
    
    @Test
    public void testBindsToRequestedPort() throws IOException {
        assertEquals(State   .sharedState()
                             .getMaster()
                             .getGlobalConfig()
                             .get("net.remotefs.port"),
                     listener.listenSocket
                             .socket()
                             .getLocalPort());
    }
    
    @Test
    public void testRebindsSocketIfConfigNetRemoteAddressChanged() throws IOException {
        String newAddress = "127.1.2.3"; // may not be portable; rewrite test if not
        setRemotefsAddress(newAddress);
        
        Util.waitUntil(100, ()->{
            return listener.listenAddress().equals(newAddress);
        });
        
        assertEquals(newAddress, listener.listenAddress());
    }
    
    @Test
    public void testRebindsSocketIfConfigNetRemotePortChanged() throws IOException {
        int newPort = testPort() + 1;
        setRemotefsPort(newPort);
        
        Util.waitUntil(100, ()->{
            return listener.listenPort() == newPort;
        });
        
        assertEquals(newPort, listener.listenPort());
    }
    
    @Test
    public void testDoesNotRebindSocketIfConfigNetRemoteAddressChangedToIdenticalValue() throws IOException {
        ServerSocketChannel socket = listener.listenSocket;
        setRemotefsAddress(listener.listenAddress());
        
        Util.sleep(10);
        assertEquals(socket, listener.listenSocket);
    }
    
    @Test
    public void testDoesNotRebindSocketIfConfigNetRemotePortChangedToIdenticalValue() throws IOException {
        ServerSocketChannel socket = listener.listenSocket;
        setRemotefsPort(listener.listenPort());
        
        Util.sleep(10);
        assertEquals(socket, listener.listenSocket);
    }
    
    @Test
    public void testDoesNotOpenSocketIfConfigNetRemoteAddressChangedAndDisabled() throws IOException {
        setRemotefsEnabled(false);
        setRemotefsAddress("0.0.0.0");
        
        Util.sleep(10);
        assertFalse(listener.isListening());
    }
    
    @Test
    public void testDoesNotOpenSocketIfConfigNetRemotePortChangedAndDisabled() throws IOException {
        setRemotefsEnabled(false);
        setRemotefsPort(TEST_PORT+1);
        
        Util.sleep(10);
        assertFalse(listener.isListening());
    }
    
    @Test
    public void testClosesSocketIfConfigNetRemoteEnabledSetFalseFromTrue() throws IOException {
        setRemotefsEnabled(false);
        assertFalse(listener.isListening());
    }
    
    @Test
    public void testOpensSocketIfConfigNetRemoteEnabledSetTrueFromFalse() throws IOException {
        setRemotefsEnabled(false);
        setRemotefsEnabled(true);
        assertTrue(listener.isListening());
    }
    
    @Test
    public void testListenThreadAcceptsConnections() throws IOException {
        LinkedList<SocketChannel> clients = new LinkedList<>();
        
        for(int i = 0; i < 10; i++) {
            SocketChannel     client  = SocketChannel.open();
            InetSocketAddress address = new InetSocketAddress(testAddress(),
                                                              testPort());
            
            client.connect(address);
            clients.add(client);
        }
        
        for(SocketChannel client : clients) {
            client.close();
        }
    }
    
    @Test
    public void testCloseTerminatesOpenConnections() throws IOException {
        LinkedList<SocketChannel> clients = new LinkedList<>();
        
        for(int i = 0; i < 10; i++) {
            SocketChannel     client  = SocketChannel.open();
            InetSocketAddress address = new InetSocketAddress(testAddress(),
                                                              testPort());
            
            client.connect(address);
            clients.add(client);
        }
        
        listener.close();
        
        ByteBuffer buf = ByteBuffer.allocate(1);
        for(SocketChannel client : clients) {
            try {
                assertEquals(-1, client.read(buf));
            } catch(SocketException exc) {}
        }
    }
    
    @Test
    public void testCloseClosesSocket() throws IOException {
        listener.close();
        assertFalse(listener.isListening());
        assertNull (listener.listenSocket);
        
        SocketChannel     client  = SocketChannel.open();
        InetSocketAddress address = new InetSocketAddress(testAddress(),
                                                          testPort());
        
        try {
            client.connect(address);
            fail();
        } catch(SocketException exc) {}
    }
    
    @Test
    public void testListenAddressShowsLocalSocketAddress() {
        assertEquals(testAddress(), listener.listenAddress ());
        assertEquals(testAddress(), listener.listenSocket
                                            .socket        ()
                                            .getInetAddress()
                                            .getHostAddress());
    }
    
    @Test
    public void testListenPortShowsLocalBindPort() {
        assertEquals(testPort(), listener.listenPort());
        assertEquals(testPort(), listener.listenSocket
                                         .socket()
                                         .getLocalPort());
    }
}
