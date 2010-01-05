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
public class ByteType extends NumericTypeBase<Byte>
{
    public static final String INDEX_NAME = "hg_byte_value_index";
  
    protected String getIndexName()
    {
        return INDEX_NAME;
    }
    
    protected byte [] writeBytes(Byte value)
    {
        byte [] data = new byte[1];
        data[0] = value.byteValue();
        return data;
    }
     
    protected Byte readBytes(byte [] bytes, int offset)
    {
        return new Byte(bytes[offset]);
    } 
}



