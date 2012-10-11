/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import java.util.HashMap;
import java.util.Map;

import org.hypergraphdb.HGHandleFactory;

import org.hypergraphdb.HGPersistentHandle;

public class BAtoHandle implements ByteArrayConverter<HGPersistentHandle>
{
    private static final Map<HGHandleFactory, BAtoHandle> M = 
        new HashMap<HGHandleFactory, BAtoHandle>();
    
    private HGHandleFactory handleFactory = null;
    
    public synchronized static ByteArrayConverter<HGPersistentHandle> getInstance(HGHandleFactory handleFactory)
    {
        BAtoHandle instance = M.get(handleFactory);
        if (instance == null)
        {
            instance = new BAtoHandle();
            instance.handleFactory = handleFactory;
            M.put(handleFactory, instance);
        }
        return instance;
    }
    
    public byte[] toByteArray(HGPersistentHandle object)
    {
        return object.toByteArray();
    }

    public HGPersistentHandle fromByteArray(byte[] byteArray, int offset, int length)
    {
        return handleFactory.makeHandle(byteArray, offset);
//        return UUIDPersistentHandle.makeHandle(byteArray);
    }
}