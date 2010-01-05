/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.handle;

import org.hypergraphdb.HGPersistentHandle;

public class DefaultManagedLiveHandle extends DefaultLiveHandle implements HGManagedLiveHandle
{
	protected long retrievalCount;
	protected long lastAccessTime;
	
	public DefaultManagedLiveHandle(Object ref, 
						   	   HGPersistentHandle pHandle,
						   	   byte flags,
						   	   long retrievalCount,
						   	   long lastAccessTime)
	{
		super(ref, pHandle, flags);
		this.retrievalCount = retrievalCount;
		this.lastAccessTime = lastAccessTime;
	}

	public void accessed()
	{
		retrievalCount++;
		lastAccessTime = System.currentTimeMillis();
	}
	
	public final long getLastAccessTime() 
	{
		return lastAccessTime;
	}

	public final void setLastAccessTime(long lastAccessTime) 
	{
		this.lastAccessTime = lastAccessTime;
	}

	public final long getRetrievalCount() 
	{
		return retrievalCount;
	}

	public final void setRetrievalCount(long retrievalCount) 
	{
		this.retrievalCount = retrievalCount;
	}
}
