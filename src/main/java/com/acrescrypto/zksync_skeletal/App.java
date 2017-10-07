package com.acrescrypto.zksync_skeletal;

import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import com.acrescrypto.zksync.crypto.Ciphersuite;
import com.acrescrypto.zksync.crypto.Key;

public class App 
{
    public static void main( String[] args )
    {
		Security.addProvider(new BouncyCastleProvider());
    }
    
    public static void hexdump(String caption, byte[] data) {
    	System.out.printf("%s (%d bytes)\n", caption, data.length);
		for(int i = 0; i <= 16 * (int) Math.ceil((double) data.length/16); i++) {
			if((i % 16) == 0) {
				if(i != 0) {
					System.out.print("  |");
					for(int j = 0; j < 16; j++) {
						byte b = i+j-16 < data.length ? data[i+j-16] : 0;
						char c = '.';
						if(b >= 0x20 && b < 0x80) c = (char) b;
						System.out.printf("%c", c);
					}
					System.out.println("|");
				}
				
				System.out.printf("%04x  ", i);
			}
			
			if(i < data.length) System.out.printf("%02x", data[i]);
			else System.out.print("  ");
			if((i % 2) == 1) System.out.print(" ");
			if((i % 8) == 7) System.out.print(" ");
		}
		
		System.out.println();
		System.out.println();
    }
}
