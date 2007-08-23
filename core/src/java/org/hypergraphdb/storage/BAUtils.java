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
        return buffer[offset + 0] + 
        	   256*buffer[offset + 1] + 
        	   256*256*buffer[offset + 2] + 
        	   256*256*256*buffer[offset + 3];		
	}
	
	public static void writeInt(int c, byte [] buffer, int offset)
	{
		buffer[offset + 0] = (byte)(c % 256);
        c = c >> 8;
        buffer[offset + 1] = (byte)(c % 256);
        c = c >> 8;
        buffer[offset + 2] = (byte)(c % 256);
        c = c >> 8;
        buffer[offset + 3] = (byte)(c % 256);		
	}
}