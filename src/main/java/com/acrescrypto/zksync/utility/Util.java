package com.acrescrypto.zksync.utility;

import java.nio.ByteBuffer;

import com.acrescrypto.zksync.crypto.HashContext;

public class Util {
	public static void hexdump(String caption, byte[] data) {
		System.out.printf("%s (%d bytes, fingerprint %s)\n", caption, data.length, fingerprint(data));
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

	public static String fingerprint(byte[] data) {
		return bytesToHex((new HashContext(data)).finish()).substring(0, 8);
	}

	public static byte[] hexToBytes(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
					+ Character.digit(s.charAt(i+1), 16));
		}
		return data;
	}

	public static String bytesToHex(byte[] b) {
		StringBuilder sb = new StringBuilder();
		for(int i = 0; i < b.length; i++) sb.append(String.format("%02x", b[i]));
		return sb.toString();
	}

	public static byte[] truncateArray(byte[] array, int length) {
		byte[] newArray = new byte[length];
		for(int i = 0; i < length; i++) newArray[i] = array[i];
		return newArray;
	}
	
	public static long unsignInt(int intVal) {
		return intVal >= 0 ? intVal : intVal + 0x100000000l;
	}
	
	public static int unsignShort(short shortVal) {
		return shortVal >= 0 ? shortVal : shortVal + 0x10000;
	}
	
	public static short unsignByte(byte byteVal) {
		return (short) (byteVal >= 0 ? byteVal : byteVal + 0x100);
	}
	
	public static long shortTag(byte[] tag) {
		return ByteBuffer.wrap(tag).getLong();
	}

	public static boolean isLinux() {
		return System.getProperty("os.name").equals("Linux");
	}

	public static boolean isWindows() {
		return false; // TODO: implement
	}

	public static boolean isOSX() {
		return System.getProperty("os.name").equals("Mac OS X");
	}
}
