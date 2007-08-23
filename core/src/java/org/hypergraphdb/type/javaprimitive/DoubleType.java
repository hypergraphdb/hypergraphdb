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
public class DoubleType extends PrimitiveTypeBase
{
    public static final String INDEX_NAME = "hg_double_value_index";
 
    private static final DoubleComparator comp = new DoubleComparator();
    
    public static class DoubleComparator implements Comparator<byte[]>
    {
        public int compare(byte [] left, byte [] right)
        {
            byte [] left_f = new byte[left.length - dataOffset];
            System.arraycopy(left, dataOffset, left_f, 0, left_f.length);
            byte [] right_f = new byte[right.length - dataOffset];
            System.arraycopy(right, dataOffset, right_f, 0, right_f.length);
            
            return Double.compare(bytesToDouble(left_f), bytesToDouble(right_f));
            
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
        byte [] data = new byte[8];
        long v = Double.doubleToLongBits(((Double)value).doubleValue());
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
    
    protected Object readBytes(byte [] bytes, int offset)
    {
        return new Double(bytesToDouble(bytes));
    }
    
    private static double bytesToDouble(byte[] bytes)
    {
        int l = bytes.length - 8;
        return Double.longBitsToDouble((((long)bytes[l] << 56) +
                ((long)(bytes[l+1] & 255) << 48) +
                ((long)(bytes[l+2] & 255) << 40) +
                ((long)(bytes[l+3] & 255) << 32) +
                ((long)(bytes[l+4] & 255) << 24) +
                ((bytes[l+5] & 255) << 16) + 
                ((bytes[l+6] & 255) <<  8) + 
                ((bytes[l+7] & 255) <<  0)));
    }
 
 }