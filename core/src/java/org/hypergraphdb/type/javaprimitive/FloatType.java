/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type.javaprimitive;

import java.util.Comparator;

/**
 *
 * @author  User
 */
public class FloatType extends PrimitiveTypeBase
{
    public static final String INDEX_NAME = "hg_float_value_index";
 
    private static final FloatComparator comp = new FloatComparator();
    
    public static class FloatComparator implements Comparator<byte[]>
    {
        public int compare(byte [] left, byte [] right)
        {
            byte [] left_f = new byte[4];
            System.arraycopy(left, dataOffset, left_f, 0, 4);
            byte [] right_f = new byte[4];
            System.arraycopy(right, dataOffset, right_f, 0, 4);                        
            return Float.compare(bytesToFloat(left_f), bytesToFloat(right_f));
        }
    }
     
    public Comparator<byte[]> getComparator()
    {
        return comp;
    }
    
    protected String getIndexName()
    {
        return INDEX_NAME;
    }
    
    protected byte [] writeBytes(Object value)
    {
        byte [] data = new byte[4];
        int i = Float.floatToIntBits(((Float)value).floatValue());
        data[3] = (byte) (i >>> 0);
        data[2] = (byte) (i >>> 8);
        data[1] = (byte) (i >>> 16);
        data[0] = (byte) (i >>> 24);
        return data;
    }
    
    protected Object readBytes(byte [] bytes, int offset)
    {
        return new Float(bytesToFloat(bytes));
    }
    
    private static float bytesToFloat(byte[] b)
    {
    	int l = b.length - 4;    	 
        int i = ((b[l + 3] & 0xFF) << 0) +
		((b[l + 2] & 0xFF) << 8) +
		((b[l+ 1] & 0xFF) << 16) +
		((b[l + 0]) << 24);
	    return Float.intBitsToFloat(i);
    }
 }