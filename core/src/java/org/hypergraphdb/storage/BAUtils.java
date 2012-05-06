/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
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
  public static long readLong(byte [] data, int offset)
  {
  	return readUnsignedLong(data, offset) ^ 0x8000000000000000L;
  }
  
  private static long readUnsignedLong(byte [] data, int offset)
  {
    return (((long)(data[offset] & 255) << 56) +
            ((long)(data[offset + 1] & 255) << 48) +
            ((long)(data[offset + 2] & 255) << 40) +
            ((long)(data[offset + 3] & 255) << 32) +
            ((long)(data[offset + 4] & 255) << 24) +
            ((long)(data[offset + 5] & 255) << 16) + 
            ((long)(data[offset + 6] & 255) <<  8) + 
            ((long)(data[offset + 7] & 255) <<  0)); 
  }
  

  public static void writeLong(long v, byte [] data, int offset)
  {
    writeUnsignedLong(v ^ 0x8000000000000000L, data, offset);
  }
  
  public static void writeUnsignedLong(long v, byte [] data, int offset)
  {
    data[offset] = (byte) ((v >>> 56)); 
    data[offset + 1] = (byte) ((v >>> 48));
    data[offset + 2] = (byte) ((v >>> 40)); 
    data[offset + 3] = (byte) ((v >>> 32));
    data[offset + 4] = (byte) ((v >>> 24)); 
    data[offset + 5] = (byte) ((v >>> 16));
    data[offset + 6] = (byte) ((v >>> 8)); 
    data[offset + 7] = (byte) ((v >>> 0));
  }
    
	public static int readInt(byte[] data, int offset)
	{
    return (int) (readUnsignedInt(data, offset) ^ 0x80000000);
	}
	
	public static long readUnsignedInt(byte[] data, int offset)
	{
    long c1 = (data[offset] & 0xFF);
    long c2 = (data[offset + 1] & 0xFF);
    long c3 = (data[offset + 2] & 0xFF);
    long c4 = (data[offset + 3] & 0xFF);
    return ((c1 << 24) | (c2 << 16) | (c3 << 8) | c4);
	}
	
	public static void writeInt(int val, byte[] data, int offset)
	{
    writeUnsignedInt(val ^ 0x80000000, data, offset);
	}
	
	public static void writeUnsignedInt(long c, byte[] data, int offset)
	{
		data[offset + 0] = (byte) ((c >>> 24) & 0xFF);
		data[offset + 1] = (byte) ((c >>> 16) & 0xFF);
		data[offset + 2] = (byte) ((c >>> 8) & 0xFF);
		data[offset + 3] = (byte) ((c >>> 0) & 0xFF);
	}
	
	public static boolean eq(byte[] left, int leftPos, byte[] right, int rightPos, int size)
	{
		int i = leftPos, j = rightPos;
		if (leftPos + size > left.length)
			return false;
		if (rightPos + size > right.length)
			return false;
		while (size > 0)
		{
			if (left[i++] != right[j++])
				return false;
			size--;
		}
		return true;
	}
	
	public static int compare(byte[] left, int leftPos, byte[] right, int rightPos, int max)
	{
		int maxLeft = leftPos + max;
		int maxRight = rightPos + max;
		int i = leftPos;
		int j = rightPos;
		int comp = 0;
		while (comp == 0 && i < maxLeft && j < maxRight)
			comp = left[i++] - right[j++];
		return comp;
	}
}
