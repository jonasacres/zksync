package com.acrescrypto.zksync.net.noise;

public class NoiseHandshakes {
	// protocol strings taken from Noise protocol specification
	// http://noiseprotocol.org/noise.html
	// revision 34, 2018-07-11
	
	public final static String XX
	   = "XX:\n"
	   + "  -> e\n"
	   + "  <- e, ee, s, es\n"
	   + "  -> s, se\n";
	
	public final static String XKpsk3
	   = "XKpsk3:\n"
	   + "  <- s\n"
	   + "  ...\n"
	   + "  -> e, es\n"
	   + "  <- e, ee\n"
	   + "  -> s, se, psk\n";
}
