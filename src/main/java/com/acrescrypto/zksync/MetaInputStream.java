package com.acrescrypto.zksync;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

public class MetaInputStream extends InputStream {
	public Queue<InputStream> streams;
	public InputStream current;
	public boolean finished;
	
	public MetaInputStream() {
		this.streams = new LinkedList<InputStream>();
	}
	
	public MetaInputStream(InputStream[] streams) {
		this.streams = new LinkedList<InputStream>();
		for(InputStream stream : streams) this.streams.add(stream);
		current = streams.length == 0 ? null : this.streams.remove();
	}
	
	public MetaInputStream(Collection<InputStream> streams) {
		this.streams = new LinkedList<InputStream>(streams);
		try {
			current = this.streams.remove();
		} catch(NoSuchElementException exc) {
			current = null;
		}
	}
	
	public void add(InputStream stream) {
		if(finished) throw new RuntimeException("attempted to add to finished MetaInputStream");
		if(current == null) current = stream;
		else streams.add(stream);
	}
	
	public void add(byte[] bytes) {
		if(finished) throw new RuntimeException("attempted to add to finished MetaInputStream");
		add(new ByteArrayInputStream(bytes.clone()));
	}
	
	public void finish() {
		finished = true;
		this.notifyAll();
	}
	
	public InputStream nextStream() {
		while(streams.isEmpty() && !finished) {
			try {
				this.wait();
			} catch (InterruptedException exc) {}
		}
		
		return streams.remove();
	}

	@Override
	public int read() throws IOException {
		try {
			int r = current.read();
			while(r == -1) {
				current = nextStream();
				r = current.read();
			}
			
			return r;
		} catch(NullPointerException | NoSuchElementException exc) {
			return -1;
		}
	}
	
	@Override
	public int read(byte[] buf, int off, int len) throws IOException {
		int bytesRead = 0;
		
		if(current == null) return -1;
		try {
			while(bytesRead < len) {
				int r = current.read(buf, off+bytesRead, len);
				if(r == -1) {
					current = nextStream();
				}
			}
		} catch(NullPointerException | NoSuchElementException exc) {
			if(bytesRead == 0) return -1;
		}
		
		return bytesRead;
	}
	
	@Override
	public int available() throws IOException {
		return current.available();
	}
	
	// TODO: consider a faster skip; not calling it yet though...
	
	@Override
	public boolean markSupported() {
		return false;
	}
}
