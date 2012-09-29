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
 * <p>
 * The implementation of the primitive <code>Integer</code> type.
 * </p>
 *
 * <p>
 * A <code>java.lang.Integer</code> object is translated to a byte []  as
 * follows:
 *
 * <ul>
 * <li>The first 4 bytes consitute an unsigned integer - the reference count
 * for the string. The reference count is managed by the superclass</li>
 * <li>The rest of the bytes constitute the actual string with the default 8-bit
 * Java encoding.</li>
 * </ul>
 *
 * </p>
 *
 * @author Borislav Iordanov
 */
public final class IntType extends NumericTypeBase<Integer>
{
    public static final String INDEX_NAME = "hg_int_value_index";
    
    private static final IntComparator comp = new IntComparator();
    
    public static class IntComparator implements Comparator<byte[]>, java.io.Serializable
    {
        private static final long serialVersionUID = 1L;
        public int compare(byte [] left, byte [] right)
        {
            Integer l = bytesToInt(left, dataOffset), r = bytesToInt(right, dataOffset);
            return l.compareTo(r);
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
    
    protected byte [] writeBytes(Integer value)
    {
        byte [] data = new byte[4];
        int v = value.intValue();
        data[0] = (byte) ((v >>> 24) & 0xFF); 
        data[1] = (byte) ((v >>> 16) & 0xFF);
        data[2] = (byte) ((v >>> 8) & 0xFF); 
        data[3] = (byte) ((v >>> 0) & 0xFF);
        return data;
    }
    
    protected Integer readBytes(byte [] bytes, int offset)
    {
        return bytesToInt(bytes, offset);
    }
    
    protected static Integer bytesToInt(byte [] bytes, int offset)
    {
        int ch1 = bytes[offset];
        int ch2 = bytes[offset + 1];
        int ch3 = bytes[offset + 2];
        int ch4 = bytes[offset + 3];
        int i = ((ch1 & 0xFF) << 24)
	      | ((ch2 & 0xFF) << 16)
	      | ((ch3 & 0xFF) << 8)
	      | (ch4 & 0xFF);
        return new Integer(i);
       // return new Integer(((ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0)));
    }
}
