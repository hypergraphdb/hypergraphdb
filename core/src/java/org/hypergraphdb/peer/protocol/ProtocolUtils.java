package org.hypergraphdb.peer.protocol;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class ProtocolUtils {

	public static boolean verifySignature(InputStream in, byte[] signature) {
		byte[] streamData = new byte[signature.length];
		
		try{
			if (in.read(streamData) == signature.length){
				return Arrays.equals(streamData, signature);
			}
		}catch(IOException ex){
			ex.printStackTrace();
		}
		
		return false;
	}
	public static void writeSignature(OutputStream out, byte[] signature){
		try{
			out.write(signature);
		}catch(IOException ex){
			ex.printStackTrace();
		}
	}
}
