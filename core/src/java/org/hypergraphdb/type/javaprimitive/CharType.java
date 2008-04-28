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
public class CharType extends NumericTypeBase<Character>
{
    
    public static final String INDEX_NAME = "hg_char_value_index";
    
    protected String getIndexName()
    {
        return INDEX_NAME;
    }
    
    protected byte [] writeBytes(Character value)
    {
        byte [] data = new byte[2];
        int v = value.charValue();
        data[0] = (byte) ((v >>> 8) & 0xFF); 
        data[1] = (byte) ((v >>> 0) & 0xFF);
        return data;
    }
    
    protected Character readBytes(byte [] bytes, int offset)
    {
        int ch1 = bytes[offset];
        int ch2 = bytes[offset + 1];
        int tot = (ch1 << 8) + (ch2 << 0);
        return new Character((char) tot);     
    }

 }
