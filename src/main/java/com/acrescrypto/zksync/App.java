package com.acrescrypto.zksync;

import java.security.Security;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

public class App 
{
    public static void main( String[] args )
    {
		Security.addProvider(new BouncyCastleProvider());
    }
}
