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
        LinkedList<ZKFSRemoteMessageIncoming> messages = new LinkedList<>();
        
        @Override public void processMessage(ZKFSRemoteMessageIncoming msg) { messages.add(msg); }
        @Override public void setChannelId(long channelId) { this.channelId = channelId; }
        @Override public long channelId() { return channelId; }
        @Override public void close() throws IOException {}
        
        public ZKFSRemoteMessageIncoming messageWithId(long msgId) {
            for(ZKFSRemoteMessageIncoming message : messages) {
                if(message.msgId() == msgId) return message;
            }
            
            return null;
        }
    }
    
    class OutgoingMessage {
        long                       channelId,
                                   msgId,
                                   fragments;
        int                        cmd;
        ExampleListener            cListener;
        
        public OutgoingMessage(long channelId, long msgId, int cmd) {
            this.channelId         = channelId;
            this.msgId             = msgId;
            this.cmd               = cmd;
            this.cListener         = (ExampleListener) connection.listenerForChannel(channelId);
        }
        
        public void sendBytes(int length, boolean finished) throws IOException {
            CryptoSupport crypto   = listener.state().getMaster().getCrypto();
            byte[]        payload  = crypto.expand(
                                               seed(),
                                               length,
                                               Util.serializeLong(fragments),
                                               new byte[0]);
            ByteBuffer    header   = ByteBuffer.allocate(3*8 + 1*4);
            long          msgLen   = header.capacity() + length;
            long          maskedId = msgId
                                   | (finished
                                      ? (1 << 63)
                                      : 0
                                     );
            
            header.putLong(msgLen);
            header.putLong(maskedId);
            header.putLong(channelId);
            header.putInt (fragments == 0 ? cmd : 0);
            header.flip();
            
            clientSocket.write(header);
            clientSocket.write(ByteBuffer.wrap(payload));
            
            fragments++;
            expect(msgId, payload, finished);
        }
        
        public void expect(long msgId, byte[] data, boolean finished) throws IOException {
            if(cListener == null || !(cListener instanceof ExampleListener)) return;
            
            assertTrue   (Util.waitUntil(100, ()->cListener.messageWithId(msgId) != null));
            
            ZKFSRemoteMessageIncoming msg = cListener.messageWithId(msgId);
            assertNotNull(msg);
            assertEquals (cmd,       msg.cmd());
            assertEquals (channelId, msg.channelId());
            
            assertTrue   (Util.waitUntil(100, ()->msg.hasBytes(data.length)));
            
            msg.read(data.length, buf->{
                byte[]    bytes    = new byte[data.length];
                buf.get(bytes);
                
                assertArrayEquals(bytes, data);
            });
            
            assertEquals (finished, msg.isSenderFinished());
        }
        
        public byte[] seed() {
            return Util.concat(
                    Util.serializeLong(channelId),
                    Util.serializeLong(msgId),
                    Util.serializeLong(fragments)
                  );
        }
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
                111);
        
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
        
        long                      msgLen       = header.getLong(),
                                  msgId        = header.getLong();
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
    
    public ExampleListener prepareTestChannelListener() {
        ExampleListener cListener = new ExampleListener();
        connection.registerChannel(cListener);
        
        return cListener;
    }
    
    public void sendMessageToChannel(long channelId, long msgId, int cmd, ByteBuffer payload) throws IOException {
        ByteBuffer                  header     = ByteBuffer.allocate(3*8 + 1*4);
        int                         msgLen     = header.capacity() + payload.remaining();
        
        header.put(Util.serializeLong(msgLen));
        header.put(Util.serializeLong(msgId));
        header.put(Util.serializeLong(channelId));
        header.put(Util.serializeInt (cmd));
        
        clientSocket.write(header);
        clientSocket.write(payload);
    }
    
    public void spamChannels(int numChannels, int numMessages, int numFragments, int maxFragmentLen) throws IOException {
        CryptoSupport               crypto     = listener.state().getMaster().getCrypto();
        LinkedList<ExampleListener> channels   = new LinkedList<>();
        LinkedList<OutgoingMessage> messages   = new LinkedList<>();
        
        for(int i = 0; i < numChannels; i++) {
            channels.add(prepareTestChannelListener());
        }
        
        for(int i = 0; i < numMessages; i++) {
            int                    chanIndex   = crypto.defaultPrng().getInt(channels.size() + 1);
            long                   channelId   = chanIndex < channels.size()
                                                 ? channels.get(chanIndex).channelId()
                                                 : crypto.defaultPrng().getLong();
            
            messages.add(new OutgoingMessage(channelId, 1000 + i, i));
        }
        
        for(int i = 0; i < numFragments; i++) {
            int                    length      = crypto.defaultPrng().getInt(maxFragmentLen),
                                   msgIndex    = crypto.defaultPrng().getInt(messages.size());
            OutgoingMessage        message     = messages.get(msgIndex);
            
            System.out.printf("spamChannels(): fragment=%d, msgId=%d, channelId=%d, length=%d\n",
                    i,
                    message.msgId,
                    message.channelId,
                    length);
            message.sendBytes(length, false);
        }
        
        for(OutgoingMessage message : messages) {
            int                    length      = crypto.defaultPrng().getInt() % 2 == 0
                                                 ? 0
                                                 : crypto.defaultPrng().getInt(maxFragmentLen);
            message.sendBytes(length, true);
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
    public void testDispatchesIncomingMessages() throws IOException {
        authenticate();
        spamChannels(   4,
                     1024,
                     4096,
                     1 << 18); 
    }
}
