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
public class DoubleType extends PrimitiveTypeBase<Double>
{
    public static final String INDEX_NAME = "hg_double_value_index";
 
    private static final DoubleComparator comp = new DoubleComparator();
    
    public static class DoubleComparator implements Comparator<byte[]>, java.io.Serializable
    {
		private static final long serialVersionUID = 1L;    	
        public int compare(byte [] left, byte [] right)
        {
            return Double.compare(bytesToDouble(left, dataOffset), bytesToDouble(right, dataOffset));            
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
    
    protected byte [] writeBytes(Double value)
    {
        byte [] data = new byte[8];
        long v = Double.doubleToLongBits(value.doubleValue());
        data[0] = (byte) ((v >>> 56)); 
        data[1] = (byte) ((v >>> 48));
        data[2] = (byte) ((v >>> 40)); 
        data[3] = (byte) ((v >>> 32));
        data[4] = (byte) ((v >>> 24)); 
        data[5] = (byte) ((v >>> 16));
        data[6] = (byte) ((v >>> 8)); 
        data[7] = (byte) ((v >>> 0));
        return data;
    }
    
    protected Double readBytes(byte [] bytes, int offset)
    {
        return new Double(bytesToDouble(bytes, offset));
    }
    
    private static double bytesToDouble(byte[] bytes, int offset)
    {
        //int l = bytes.length - 8;
        return Double.longBitsToDouble((((long)bytes[offset] << 56) +
                ((long)(bytes[offset+1] & 255) << 48) +
                ((long)(bytes[offset+2] & 255) << 40) +
                ((long)(bytes[offset+3] & 255) << 32) +
                ((long)(bytes[offset+4] & 255) << 24) +
                ((bytes[offset+5] & 255) << 16) + 
                ((bytes[offset+6] & 255) <<  8) + 
                ((bytes[offset+7] & 255) <<  0)));
    }
 }