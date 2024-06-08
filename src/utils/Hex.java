package utils;

import java.util.Arrays;

public class Hex {
	static final String hexits = "0123456789ABCDEF";
	
	public static String of( byte[] data ) {
		return of( data, data.length);
	}
	
	public static String of( byte[] data, int len ) {
		var sb = new StringBuilder();
		for ( var b : Arrays.copyOf(data, len) ) {
			sb.append( hexits.charAt( (b & 0xF0 ) >>> 4 ));;
			sb.append( hexits.charAt( (b & 0x0F ) ));
		}		
		return sb.toString();
	}
}
