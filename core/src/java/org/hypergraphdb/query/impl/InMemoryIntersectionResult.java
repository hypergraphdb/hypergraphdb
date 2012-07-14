/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGRandomAccessResult;
//import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.util.ArrayBasedSet;

@SuppressWarnings("unchecked")
public class InMemoryIntersectionResult<T> implements HGRandomAccessResult<T>//, RSCombiner<T>
{
	private HGRandomAccessResult<T> left, right;
	private HGRandomAccessResult<T> intersection = null;
	
	private void intersect()
	{
		if (intersection != null)
			return;
		ArrayBasedSet<Object> set = new ArrayBasedSet<Object>(new Object[0]);
		ZigZagIntersectionResult<T> zigzag = new ZigZagIntersectionResult<T>(left, right);
		while (zigzag.hasNext())
			set.add(zigzag.next());
		intersection = (HGRandomAccessResult<T>)set.getSearchResult();		
		left.close();
		right.close();
		left = right = null;		
	}
	
	public InMemoryIntersectionResult(HGRandomAccessResult<T> left, HGRandomAccessResult<T> right)
	{
		this.left = left;
		this.right = right;
	}
	
	public void goBeforeFirst()
	{
	    intersect();
	    intersection.goBeforeFirst();
	}
	
	public void goAfterLast()
	{
	    intersect();
	    intersection.goAfterLast();
	}
	
	public GotoResult goTo(T value, boolean exactMatch)
	{
		intersect();
		return intersection.goTo(value, exactMatch);
	}

	public void close()
	{
		if (intersection != null)
			intersection.close();
	}

	public T current()
	{
		intersect();
		return intersection.current();
	}

	public boolean isOrdered()
	{
		return true;
	}

	public boolean hasPrev()
	{
		intersect();
		return intersection.hasPrev();
	}

	public T prev()
	{
		intersect();
		return intersection.prev();
	}

	public boolean hasNext()
	{
		intersect();
		return intersection.hasNext();
	}

	public T next()
	{
		intersect();
		return intersection.next();
	}

	public void remove()
	{
		intersect();
		intersection.remove();
	}

	public final static class Combiner<T> implements RSCombiner<T>
	{
		public HGSearchResult<T> combine(HGSearchResult<T> left, HGSearchResult<T> right)
		{
			return new InMemoryIntersectionResult<T>((HGRandomAccessResult<T>)left, (HGRandomAccessResult<T>)right);
		}
	}
	
//	public void init(HGSearchResult<T> l, HGSearchResult<T> r)
//	{
//		this.left = (HGRandomAccessResult<T>)l;
//		this.right = (HGRandomAccessResult<T>)r;
//	}
	
//	public void reset() {
//		intersection = null;
//	}
}