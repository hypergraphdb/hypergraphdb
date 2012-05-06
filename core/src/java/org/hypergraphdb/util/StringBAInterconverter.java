package org.hypergraphdb.util;

public class StringBAInterconverter 
{
	public static String byteA2String(byte[] ba) 
	{
		char[] ca = new char[ba.length/2];
		for (int i = 0; i < ca.length; i++)
		{ 
			int hb = (ba[i*2+1] << 8) & 0x0000ff00;   //thanks olli for the hint
			int lb = ba[i*2] & 0x000000ff;
	
			ca[i] = (char )(hb | lb);	    	  
		}

		return new String(ca);
	}

	public static String byteA2String(byte[] ba, int offset) 
	{
		char[] ca = new char[(ba.length-offset)/2];
	     for (int i = 0; i < ca.length; i++)
	     { 
	 		int hb = (ba[(i*2)+1+offset] << 8) & 0x0000ff00;   
	 		int lb = ba[(i*2)+offset] & 0x000000ff;	
	 		ca[i] = (char)(hb | lb);	    	  
	     }
		return new String(ca);
	}

	
	public static byte[] string2ByteA(String in)
	{
	 char[] ca = in.toCharArray();
	 short buf;
	 byte[] ba = new byte[ca.length*2];
	 for (int i = 0; i<ca.length; i++)
	 {
		    buf = (short) ca[i];
		    ba[i*2]= (byte) buf;
		    ba[(i*2)+1]= (byte)(buf >> 8);  
	 }
	 return ba;

	}
}