/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom;

/**
 * <p>
 * This is a singleton <code>HyperGraph</code> managed atom that the system
 * uses to collect global statistics about the HyperGraph instance. Such statistics
 * are mainly used during query and storage optimization, and for the management of the 
 * lifetime of <code>HGManagedAtom</code> instances.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class HGStats 
{
	private long retrievalCount;
	private long lastAccessTime;
	
	public final void atomAccessed()
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
