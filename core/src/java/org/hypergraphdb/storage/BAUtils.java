/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.storage;

/**
 * <p>
 * This class contains some utilities methods to read/write primitively typed values from/to
 * a byte buffer.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class BAUtils 
{
	public static int readInt(byte [] buffer, int offset)
	{
        int ch1 = buffer[offset];
        int ch2 = buffer[offset + 1];
        int ch3 = buffer[offset + 2];
        int ch4 = buffer[offset + 3];
        return ((ch1 & 0xFF) << 24)
	      | ((ch2 & 0xFF) << 16)
	      | ((ch3 & 0xFF) << 8)
	      | (ch4 & 0xFF);
	}
	
	public static void writeInt(int c, byte [] buffer, int offset)
	{
		buffer[offset + 0] = (byte) ((c >>> 24) & 0xFF);
        buffer[offset + 1] = (byte) ((c >>> 16) & 0xFF);
        buffer[offset + 2] = (byte) ((c >>> 8) & 0xFF);
        buffer[offset + 3] = (byte) ((c >>> 0) & 0xFF);		
	}
}