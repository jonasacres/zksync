package com.acrescrypto.zksync.net;

import org.junit.Test;

public class PeerSwarmTest {
	@Test
	public void testPlaceholder() {
	}
	
	// openedConnection adds an open connection
	// openedConnection closes the connection automatically if the swarm is closed

	// disconnectAddress closes all connections from the indicated address
	// close disconnects all peerconnections
	
	// addPeer adds a peer to the list of connections
	// addPeer notes the peer's advertisement as connected
	// addPeer notes the peer's advertisement as known
	
	// advertiseSelf relays an advertisement to announceSelf on all connections

	// automatically connects to advertised peers
	// stops connecting to advertised peers when maxSocketCount reached
	// connection thread does not die from exceptions
	
	// waitForPage does not block if we already have the page
	// waitForPage blocks until a page is announced via receivedPage
	
	// accumulatorForTag creates an accumulator for new tags
	// accumulatorForTag returns an existing accumulator for active tags
	// accumulatorForTag no longer considers a tag active after receivedPage has been called for that tag
	
	// receivedPage announces tag to peers
}
