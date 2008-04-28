/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type.javaprimitive;


/**
 *
 * @author  User
 */
public class LongType extends NumericTypeBase<Long>
{
    
    public static final String INDEX_NAME = "hg_long_value_index";
    
    protected String getIndexName()
    {
        return INDEX_NAME;
    }
    
    protected byte [] writeBytes(Long value)
    {
        byte [] data = new byte[8];
        long v = value.longValue();
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
    
    protected Long readBytes(byte [] bytes, int offset)
    {
        return new Long((((long)bytes[offset] << 56) +
                ((long)(bytes[offset + 1] & 255) << 48) +
                ((long)(bytes[offset + 2] & 255) << 40) +
                ((long)(bytes[offset + 3] & 255) << 32) +
                ((long)(bytes[offset + 4] & 255) << 24) +
                ((bytes[offset + 5] & 255) << 16) + 
                ((bytes[offset + 6] & 255) <<  8) + 
                ((bytes[offset + 7] & 255) <<  0)));    
    }
}