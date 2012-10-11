/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * <p> 
 * A simple structure that holds system-level atom attributes.
 * </p>
 */
public final class HGAtomAttrib 
{
	byte flags = HGSystemFlags.DEFAULT;
	long retrievalCount;
	long lastAccessTime;

	static class Converter implements ByteArrayConverter<HGAtomAttrib>
	{
		static void putLong(byte [] dest, int offset, long value)
		{
	        dest[0 + offset] = (byte) ((value >>> 56)); 
	        dest[1 + offset] = (byte) ((value >>> 48));
	        dest[2 + offset] = (byte) ((value >>> 40)); 
	        dest[3 + offset] = (byte) ((value >>> 32));
	        dest[4 + offset] = (byte) ((value >>> 24)); 
	        dest[5 + offset] = (byte) ((value >>> 16));
	        dest[6 + offset] = (byte) ((value >>> 8)); 
	        dest[7 + offset] = (byte) ((value >>> 0));			
		}
		
		static long getLong(byte [] src, int offset)
		{
	        return (((long)src[offset] << 56) +
	                ((long)(src[offset + 1] & 255) << 48) +
	                ((long)(src[offset + 2] & 255) << 40) +
	                ((long)(src[offset + 3] & 255) << 32) +
	                ((long)(src[offset + 4] & 255) << 24) +
	                ((src[offset + 5] & 255) << 16) + 
	                ((src[offset + 6] & 255) <<  8) + 
	                ((src[offset + 7] & 255) <<  0));    			
		}
		
		public HGAtomAttrib fromByteArray(byte[] byteArray, int offset, int length) 
		{
			HGAtomAttrib result = new HGAtomAttrib();
			result.flags = byteArray[offset];
			result.retrievalCount = getLong(byteArray, 1 + offset);
			result.lastAccessTime = getLong(byteArray, 9 + offset);
			return result;
		}

		public byte[] toByteArray(HGAtomAttrib object) 
		{
			byte [] result = new byte[17];
			HGAtomAttrib attribs = (HGAtomAttrib)object;
			result[0] = attribs.flags;
			putLong(result, 1, attribs.retrievalCount);
			putLong(result, 9, attribs.lastAccessTime);
			return result;
		}
		
	}
	
	static final Converter baConverter = new Converter();

    public byte getFlags()
    {
        return flags;
    }

    public void setFlags(byte flags)
    {
        this.flags = flags;
    }

    public long getRetrievalCount()
    {
        return retrievalCount;
    }

    public void setRetrievalCount(long retrievalCount)
    {
        this.retrievalCount = retrievalCount;
    }

    public long getLastAccessTime()
    {
        return lastAccessTime;
    }

    public void setLastAccessTime(long lastAccessTime)
    {
        this.lastAccessTime = lastAccessTime;
    }	
}
