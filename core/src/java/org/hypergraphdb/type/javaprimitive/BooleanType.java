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
public class BooleanType extends PrimitiveTypeBase
{
    public static final String INDEX_NAME = "hg_bool_value_index";
    
    private static final BoolComparator comp = new BoolComparator();
    
    public static class BoolComparator implements Comparator<byte[]>
    {
        public int compare(byte [] left, byte [] right)
        {
            return left[dataOffset] - right[dataOffset];
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
    
    protected Object readBytes(byte [] bytes, int offset)
    {
        return new Boolean(bytes[offset] == 1);
    }
    
    protected byte [] writeBytes(Object value)
    {
       int b = ((Boolean)value).booleanValue() ? 1 : 0;
       return new byte[]{(byte) b}; 
    }

}
