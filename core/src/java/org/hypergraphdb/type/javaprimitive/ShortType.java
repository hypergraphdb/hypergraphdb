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
public class ShortType extends NumericTypeBase
{
    
    public static final String INDEX_NAME = "hg_short_value_index";
    
    protected String getIndexName()
    {
        return INDEX_NAME;
    }
    
    protected byte [] writeBytes(Object value)
    {
        short val = ((Short)value).shortValue();
        byte data [] = new byte[2];
        data[1] = (byte) (val >>> 0);
        data[0] = (byte) (val >>> 8);      
        return data;
    }
    
    protected Object readBytes(byte [] b, int offset)
    {
    	return new Short((short) (((b[offset+ 1] & 0xFF) << 0) + 
    			((b[offset]) << 8)));
    }
 }


