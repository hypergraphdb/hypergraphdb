/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.handle;

import org.hypergraphdb.HGPersistentHandle;

public class DefaultLiveHandle implements HGLiveHandle
{
	final protected HGPersistentHandle persistentHandle;
    protected Object ref;    
    protected byte flags;

	public DefaultLiveHandle(Object ref, HGPersistentHandle persistentHandle, byte flags)
    {
        this.ref = ref;
        this.persistentHandle = persistentHandle;
        this.flags = flags;
    }
    
	public final byte getFlags() 
	{
		return flags;
	}
	
    public final Object getRef()
    {
        return ref;
    }
    
    public final HGPersistentHandle getPersistentHandle()
    {
        return persistentHandle;
    }
    
    public final boolean equals(Object other)
    {
        if (other == null)
            return false;
        else if (other instanceof HGLiveHandle)
            return persistentHandle.equals(((HGLiveHandle)other).getPersistentHandle());
        else
        	return persistentHandle.equals((HGPersistentHandle)other);
    }
    
    public final int hashCode()
    {
        return persistentHandle.hashCode();
    }
}
