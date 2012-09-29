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
public class ShortType extends NumericTypeBase<Short>
{
    public static final String INDEX_NAME = "hg_short_value_index";
 
    private static final ShortComparator comp = new ShortComparator();
    
    public static class ShortComparator implements Comparator<byte[]>, java.io.Serializable
    {
        private static final long serialVersionUID = 1L;
        public int compare(byte [] left, byte [] right)
        {
            Short l = bytesToShort(left, dataOffset), r = bytesToShort(right, dataOffset);
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
    
    protected byte [] writeBytes(Short value)
    {
        short val = value.shortValue();
        byte data [] = new byte[2];
        data[1] = (byte) (val >>> 0);
        data[0] = (byte) (val >>> 8);      
        return data;
    }
    
    protected Short readBytes(byte [] b, int offset)
    {
        return bytesToShort(b, offset);
    }
    
    public static Short bytesToShort(byte [] b, int offset)
    {
    	return new Short((short) (((b[offset+ 1] & 0xFF) << 0) + 
    			((b[offset]) << 8)));
    }
}