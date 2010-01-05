/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

/**
 * 
 * <p>
 * An implementation of <code>LazyRef</code> that simply encapsulates
 * an existing value. Use it to pass a known value as a parameter to a
 * method expecting a <code>LazyRef</code>.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public final class ReadyRef<T> implements LazyRef<T> 
{
	private T value;
	
	public ReadyRef(T value)
	{
		this.value = value;
	}
	
	public T deref() 
	{
		return value;
	}
}
