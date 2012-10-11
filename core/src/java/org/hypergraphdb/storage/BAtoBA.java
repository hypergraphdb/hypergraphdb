/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

public class BAtoBA implements ByteArrayConverter<byte[]>
{
    private static final BAtoBA instance = new BAtoBA();
    
    public static ByteArrayConverter<byte[]> getInstance()
    {
        return instance;
    }
    
    public byte[] toByteArray(byte [] object)
    {
        return (byte[])object;
    }

    public byte [] fromByteArray(byte[] byteArray, int offset, int length)
    {
        if (offset == 0 && length == byteArray.length)
            return byteArray;
        else
        {
            byte [] B = new byte[length];
            System.arraycopy(byteArray, offset, B, 0, length);
            return B;
        }
    }
}
