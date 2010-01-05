/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;

public class BAtoHandle implements ByteArrayConverter<HGPersistentHandle>
{
    private static final BAtoHandle instance = new BAtoHandle();
    
    public static ByteArrayConverter<HGPersistentHandle> getInstance()
    {
        return instance;
    }
    
    public byte[] toByteArray(HGPersistentHandle object)
    {
        return ((UUIDPersistentHandle)object).toByteArray();
    }

    public HGPersistentHandle fromByteArray(byte[] byteArray)
    {
        return UUIDPersistentHandle.makeHandle(byteArray);
    }
}
