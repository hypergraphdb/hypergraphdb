/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.type.javaprimitive;

import java.util.Comparator;

/**
 *
 * @author  User
 */
public class FloatType extends PrimitiveTypeBase<Float>
{
    public static final String INDEX_NAME = "hg_float_value_index";
 
    private static final FloatComparator comp = new FloatComparator();
    
    public static class FloatComparator implements Comparator<byte[]>, java.io.Serializable
    {
		private static final long serialVersionUID = 1L;
        public int compare(byte [] left, byte [] right)
        {
            return Float.compare(bytesToFloat(left, dataOffset), bytesToFloat(right, dataOffset));
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
    
    protected byte [] writeBytes(Float value)
    {
        byte [] data = new byte[4];
        int i = Float.floatToIntBits(value.floatValue());
        data[3] = (byte) (i >>> 0);
        data[2] = (byte) (i >>> 8);
        data[1] = (byte) (i >>> 16);
        data[0] = (byte) (i >>> 24);
        return data;
    }
    
    protected Float readBytes(byte [] bytes, int offset)
    {
        return new Float(bytesToFloat(bytes, offset));
    }
    
    private static float bytesToFloat(byte[] b, int dataOffset)
    {
    	//int l = b.length - 4;    	 
        int i = ((b[dataOffset + 3] & 0xFF) << 0) +
		((b[dataOffset + 2] & 0xFF) << 8) +
		((b[dataOffset + 1] & 0xFF) << 16) +
		((b[dataOffset + 0]) << 24);
	    return Float.intBitsToFloat(i);
    }
 }