package com.acrescrypto.zksync.net.noise;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.channels.SelectableChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.nio.channels.spi.SelectorProvider;

public class SecureChannel extends AbstractSelectableChannel implements ByteChannel {
	public final static int MAX_MESSAGE_LEN = 65536;
	
	protected CipherState readState, writeState;
	protected ByteChannel channel;
	
	protected int expectedLen;
	protected ByteBuffer readBuf;
	protected boolean cachedPlaintext;
	
	public SecureChannel(ByteChannel channel, CipherState readState, CipherState writeState) {
		super(SelectorProvider.provider());
		this.readState = readState;
		this.writeState = writeState;
		this.channel = channel;
		this.readBuf = ByteBuffer.allocate(2 + MAX_MESSAGE_LEN + readState.getCrypto().symTagLength());
	}
	
	protected void readLength() {
		if(readBuf.position() >= 2) {
			int oldPos = readBuf.position();
			readBuf.rewind();
			expectedLen = (readBuf.getShort() ^ nextSipHash(readState)) & 0xffff;
			readBuf.position(oldPos);
		}
	}
	
	protected byte[] decryptCiphertext() {
		return readState.decryptWithAssociatedData(null,
				readBuf.array(),
				2,
				expectedLen);
	}
	
	protected void resetReadBuffer() {
		if(readBuf.hasRemaining()) {
			ByteBuffer newBuf = ByteBuffer.wrap(readBuf.array());
			newBuf.put(readBuf);
			readBuf = newBuf;
		} else {
			readBuf.reset();
		}
	}
	
	private int nextSipHash(CipherState state) {
		return 0; // TODO: base this off the state somehow... the hash maybe?
	}

	@Override
	public int read(ByteBuffer dst) throws IOException {
		if(cachedPlaintext) return readCachedPlaintext(dst);
		int maxBuffer = 2 + Math.max(expectedLen, 0);
		ByteBuffer limitedBuf = ByteBuffer.wrap(readBuf.array(),
				readBuf.position(),
				maxBuffer - readBuf.position());
		channel.read(limitedBuf);
		
		if(expectedLen < 0) {
			readLength();
		}
		
		if(readBuf.position() < 2 + expectedLen) return 0;
		byte[] plaintext = decryptCiphertext();
		resetReadBuffer();
		
		if(plaintext.length <= dst.remaining()) {
			dst.put(plaintext);
			return plaintext.length;
		}
		
		readBuf.put(plaintext);
		readBuf.limit(plaintext.length);
		readBuf.reset();
		cachedPlaintext = true;
		
		int r = dst.remaining();
		dst.put(readBuf.array(), 0, r);
		readBuf.position(r);
		return r;
	}
	
	protected int readCachedPlaintext(ByteBuffer dst) {
		if(dst.remaining() >= readBuf.remaining()) {
			int r = readBuf.remaining();
			dst.put(readBuf);
			readBuf.reset();
			readBuf.limit(readBuf.array().length);
			cachedPlaintext = true;
			return r;
		}
		
		int r = dst.remaining();
		dst.put(readBuf.array(), readBuf.position(), r);
		readBuf.position(readBuf.position() + r);
		return r;
	}

	@Override
	public int write(ByteBuffer src) throws IOException {
		// Credit: The idea to use SipHash to obfuscate lengths comes from NTCP2 (of the i2p project).
		int totalSent = src.remaining();
		
		while(src.remaining() > 0) {
			int chunkLen = Math.min(MAX_MESSAGE_LEN, src.remaining());
			int ctLen = writeState.getCrypto().symPaddedCiphertextSize(chunkLen);
			ByteBuffer ciphertext = ByteBuffer.allocate(2 + ctLen);

			int obfLen = (ctLen ^ nextSipHash(writeState)) & 0xffff;
			ciphertext.putShort((short) obfLen);
			ciphertext.put(writeState.encryptWithAssociatedData(null,
					src.array(),
					src.position(),
					chunkLen));
			ciphertext.rewind();
			src.position(src.position() + chunkLen);
			
			while(ciphertext.remaining() > 0) {
				channel.write(ciphertext);
			}
		}
		
		return totalSent;
	}

	@Override
	protected void implCloseSelectableChannel() throws IOException {
		channel.close();
	}

	@Override
	protected void implConfigureBlocking(boolean block) throws IOException {
		if(!(channel instanceof SelectableChannel) && !block) {
			throw new IOException("Nonblocking support not available for this channel type: " + channel.getClass());
		}
		
		((SelectableChannel) channel).configureBlocking(block);
	}

	@Override
	public int validOps() {
		if(!(channel instanceof SelectableChannel)) return 0;
		return ((SelectableChannel) channel).validOps();
	}
}
