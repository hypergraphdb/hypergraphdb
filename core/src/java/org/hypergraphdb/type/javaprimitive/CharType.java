/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
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
        char v = value.charValue();
        byte [] data = new byte[2];        
        data[1] = (byte) (v >>> 0);
        data[0] = (byte) (v >>> 8);      
        
        return data;
    }
    
    protected Character readBytes(byte [] bytes, int offset)
    {
        return new Character((char) (((bytes[offset+ 1] & 0xFF) << 0) + 
                ((bytes[offset]) << 8)));
    }
 }
