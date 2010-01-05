/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.Iterator;

/**
 * <p>
 * Implements an <code>Iterator</code> over the elements of a built-in Java array.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class ArrayBasedIterator<T> implements Iterator<T> 
{
	private T [] array;
	private int pos;
	private int end;
	
	public ArrayBasedIterator(T [] array)
	{
		this.array = array;
		pos = 0;
		if (array == null)
			end = 0;
		else
			end = array.length;
	}
	
	public ArrayBasedIterator(T [] array, int start, int end)
	{
		this.array = array;
		pos = start;
		this.end = end;
	}
	
	public boolean hasNext()
	{
		return pos < end;
	}
	
	public T next()
	{
		return array[pos++];
	}
	
	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
