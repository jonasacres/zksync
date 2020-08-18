package com.acrescrypto.zksync.fs.zkfs.remote;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;

import org.apache.commons.io.IOUtils;
import org.junit.*;

import com.acrescrypto.zksync.TestUtils;
import com.acrescrypto.zksync.crypto.CryptoSupport;
import com.acrescrypto.zksync.fs.zkfs.config.ConfigDefaults;
import com.acrescrypto.zksync.fs.zkfs.remote.ZKFSRemoteConnection.ChannelListener;
import com.acrescrypto.zksync.utility.Util;
import com.acrescrypto.zksyncweb.State;

public class ZKFSRemoteConnectionTest {
    class ExampleListener implements ChannelListener {
        long channelId;
        
        @Override public void processMessage(ZKFSRemoteMessageIncoming msg) {}
        @Override public void setChannelId(long channelId) { this.channelId = channelId; }
        @Override public long channelId() { return channelId; }
        @Override public void close() throws IOException {}
    }
    
    protected ZKFSRemoteConnection              connection;
    protected ZKFSRemoteListener                listener;
    protected SocketChannel                     clientSocket;
    
    @BeforeClass
    public static void beforeAll() {
        TestUtils.startDebugMode();
        ConfigDefaults.getActiveDefaults().set("net.remotefs.authTimeoutMs",      100);
        ConfigDefaults.getActiveDefaults().set("net.remotefs.authHashIterations", 1);
    }
    
    @Before
    public void beforeEach() throws IOException {
        State.setTestState();
        
        listener     = new ZKFSRemoteListener(State.sharedState());
        assertTrue(Util.waitUntil(100, ()->listener.isListening()));
        
        clientSocket = SocketChannel.open();
        clientSocket.connect(listener.listenSocket.getLocalAddress());
        
        assertTrue(Util.waitUntil(100, ()->listener.connections().size() > 0));
        connection   = listener.connections().stream().findFirst().get();
    }
    
    @After
    public void afterEach() throws IOException {
        State       .resetState();
        listener    .close();
        clientSocket.close();
    }
    
    @AfterClass
    public static void afterAll() {
        TestUtils     .stopDebugMode();
        ConfigDefaults.resetDefaults();
        TestUtils     .assertTidy();
    }
    
    public String testAddress() {
        return "127.0.0.1";
    }
    
    public int testPort() {
        return ZKFSRemoteListener.DEFAULT_PORT + 1;
    }
    
    public void authenticateBadly() throws IOException {
        CryptoSupport crypto      = listener.state().getMaster().getCrypto();
        ByteBuffer    saltBuf     = ByteBuffer.allocate(4+crypto.hashLength()),
                      garbage     = ByteBuffer.allocate(2*crypto.hashLength());
        
        clientSocket.read (saltBuf);
        clientSocket.write(garbage); // not a valid challenge response
        assertConnectionClosed();
    }
    
    public void authenticate() throws IOException {
        CryptoSupport crypto      = listener.state().getMaster().getCrypto();
        ByteBuffer    itrBuf      = ByteBuffer.allocate(4),
                      saltBuf     = ByteBuffer.allocate(  crypto.hashLength()),
                      response    = ByteBuffer.allocate(2*crypto.hashLength());
        
        IOUtils.readFully(clientSocket,  itrBuf);
        IOUtils.readFully(clientSocket, saltBuf);
        itrBuf.flip();
        saltBuf.flip();
        
        int          iterations   = itrBuf.getInt();
        assertEquals(iterations,    connection.authHashIterations());
        assertEquals(iterations,    listener.state()
                                            .getMaster()
                                            .getGlobalConfig()
                                            .getInt("net.remotefs.authHashIterations"));
        
        byte[]        saltBytes   = crypto.rng(crypto.hashLength()),
                      clientSalt  = Util.concat(saltBuf.array(), saltBytes),
                      serverSalt  = Util.concat(saltBytes, saltBuf.array()),
                      secret      = listener.state()
                                            .getMaster()
                                            .getGlobalConfig()
                                            .getString("net.remotefs.secret")
                                            .getBytes(),
                      clientHash  = iteratedHash(iterations,
                                                clientSalt,
                                                secret),
                      serverHash  = iteratedHash(iterations,
                                                serverSalt,
                                                secret);
        
        response.put (saltBytes);
        response.put (clientHash);
        response.flip();
        clientSocket.write(response);
        
        ByteBuffer    hashBuf     = ByteBuffer.allocate(crypto.hashLength());
        IOUtils.readFully(clientSocket, hashBuf);
        assertArrayEquals(serverHash, hashBuf.array());
    }
    
    public byte[] iteratedHash(int iterations, byte[] salt, byte[] secret) {
        CryptoSupport crypto      = listener.state().getMaster().getCrypto();
        byte[]        current     = secret;
        
        for(int i = 0; i < iterations; i++) {
            current = crypto.authenticate(salt, current);
        }
        
        return current;
    }
    
    public void assertConnectionClosed() {
        try {
            ByteBuffer readBuf = ByteBuffer.allocate(1);
            int        readLen = clientSocket.read(readBuf);
            assertEquals(-1, readLen);
        } catch(IOException exc) {
        }
    }
    
    public void performPayloadTest(boolean finished) throws IOException {
        int                       numPayloads  = 3;
        LinkedList<ByteBuffer>    payload      = new LinkedList<>();
        ZKFSRemoteMessageIncoming msg          = new ZKFSRemoteMessageIncoming(
                connection,
                1234,
                4321,
                111,
                0);
        
        for(int i = 0; i < numPayloads; i++) {
            ByteBuffer buf = ByteBuffer.allocate(i+1);
            for(int j = 0; j < i; j++) {
                buf.put((byte) j);
            }
            
            buf.flip();
            payload.add(buf);
        }
        
        authenticate();
        connection.sendResponse(msg, payload, finished);
        
        ByteBuffer                header       = ByteBuffer.allocate(2*8); 
        IOUtils.readFully(clientSocket, header);
        header.flip();
        
        long                      msgLen       = header.getLong();
        long                      msgId        = header.getLong();
        boolean                   isFinal      = (msgId & (1 << 63)) != 0;
        assertEquals(msg.msgId, msgId & ~(1 << 63));
        assertEquals(finished, isFinal);
        
        ByteBuffer                data         = ByteBuffer.allocate((int) msgLen - header.capacity());
        IOUtils.readFully(clientSocket, data);
        
        for(ByteBuffer buf : payload) {
            while(buf.hasRemaining()) {
                assertEquals(buf.get(), data.get());
            }
        }
    }
    
    @Test
    public void testConstructorSetsListener() {
        assertEquals(listener, connection.listener());
    }
    
    @Test
    public void testTxMonitorDescendsFromListener() {
        assertTrue(connection.txMonitor().hasParent(listener.txMonitor()));
    }
    
    @Test
    public void testRxMonitorDescendsFromListener() {
        assertTrue(connection.rxMonitor().hasParent(listener.rxMonitor()));
    }
    
    @Test
    public void testListensForControlChannelAtInit() {
        assertTrue(connection.hasChannel(ZKFSRemoteConnection.CHANNEL_ID_CONTROL));
    }

    @Test
    public void testRegisterChannelIssuesChannelIds() {
        ExampleListener cListener   = new ExampleListener();
        long            expectedId  = ZKFSRemoteConnection.CHANNEL_ID_USER_START;
        for(int i = 0; i < 1024; i++) {
            long        channelId   = connection.registerChannel(cListener);
            assertEquals(expectedId, channelId);
            assertEquals(expectedId, cListener.channelId());
            assertEquals(cListener,  connection.listenerForChannel(channelId));
            
            expectedId++;
        }
    }
    
    @Test
    public void testAuthenticateClosesSocketOnMismatchedHash() throws IOException {
        authenticateBadly();
    }
    
    @Test
    public void testAuthenticateClosesSocketOnTimeout() throws IOException {
        clientSocket.read(ByteBuffer.allocate(1024));
        assertConnectionClosed();
    }
    
    @Test
    public void testAuthenticateSendsCounterHashIfClientChallengeMatches() throws IOException {
        authenticate();
    }
    
    @Test
    public void testAuthenticateCountsTowardsTxBandwidth() throws IOException {
        authenticate();
        assertNotEquals(0, connection.txMonitor().getLifetimeBytes());
    }
    
    @Test
    public void testAuthenticateCountsTowardsRxBandwidth() throws IOException {
        authenticate();
        assertNotEquals(0, connection.rxMonitor().getLifetimeBytes());
    }
    
    @Test
    public void testSendResponseSerializesProperResponseWhenFinishedIsFalse() throws IOException {
        performPayloadTest(false);
    }
    
    @Test
    public void testSendResponseSerializesProperResponseWhenFinishedIsTrue() throws IOException {
        performPayloadTest(true);
    }
    
    @Test
    public void testDispatchesIncomingMessages() {
        
    }
    // testDispatchesIncomingMessages
}
