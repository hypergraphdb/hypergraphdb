/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import java.util.NoSuchElementException;
import org.hypergraphdb.HGSearchResult;

/**
 * <p>
 * Combines two ordered result sets into a (ordered) result representing their set theoretical intersection. 
 * It assumes that the objects contained in the result sets being intersected 
 * are <code>java.lang.Comparable</code> instances. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class SortedIntersectionResult<T> implements HGSearchResult<T> //, RSCombiner<T>
{
	private HGSearchResult<T> left, right;
	private T current = null, next = null, prev = null;
	private int lookahead = 0;
	
	@SuppressWarnings("unchecked")
	private T advance()
	{
		boolean advance_left = true, advance_right = true;
		Comparable<T> lnext = null, rnext = null;
		while (true)				
		{
			if (advance_left)
				if (!left.hasNext())
					return null;
				else
					lnext = (Comparable<T>)left.next();
			if (advance_right)
				if (!right.hasNext())
					return null;
				else
					rnext = (Comparable<T>)right.next();
			int comp = lnext.compareTo((T)rnext);
			if (comp == 0)
				return (T)lnext;
			else if (comp < 0) // lnext < rnext
			{
				advance_left = true;
				advance_right = false;
			}
			else
			{
				advance_left = false;
				advance_right = true;
			}				
		}
	}
	
	@SuppressWarnings("unchecked")
	private T back()
	{
		boolean back_left = true, back_right = true;
		Comparable<T> lprev = null, rprev = null;
		while (true)				
		{
			if (back_left)
				if (!left.hasPrev())
					return null;
				else
					lprev = (Comparable<T>)left.prev();
			if (back_right)
				if (!right.hasPrev())
					return null;
				else
					rprev = (Comparable<T>)right.prev();
			int comp = lprev.compareTo((T)rprev);
			if (comp == 0)
				return (T)lprev;
			else if (comp < 0) // lprev < rprev
			{
				back_left = false;
				back_right = true;
			}
			else
			{
				back_left = false;
				back_right = true;
			}				
		}		
	}
	
//	public SortedIntersectionResult()
//	{		
//	}

	public final static class Combiner<T> implements RSCombiner<T>
	{
		public HGSearchResult<T> combine(HGSearchResult<T> left, HGSearchResult<T> right)
		{
			return new SortedIntersectionResult<T>(left, right);
		}
	}
	
	public SortedIntersectionResult(HGSearchResult<T> left, HGSearchResult<T> right)
	{
		//init(left, right);
		this.left = left;
		this.right = right;
		next = advance();
		lookahead = 1;		
	}
	
//	public void reset() {
//		current = null;
//		next = null;
//		prev = null;
//		lookahead = 0;
//	}
//	
//	public void init(HGSearchResult<T> left, HGSearchResult<T> right)
//	{
//		this.left = left;
//		this.right = right;
//		next = advance();
//		lookahead = 1;
//	}
	
	public T current() 
	{
		if (current == null)
			throw new NoSuchElementException();
		else
			return current;
	}

	public void close() 
	{
		left.close();
		right.close();
	}

	public boolean hasPrev() 
	{
		return prev != null;
	}

	public T prev() 
	{
		if (prev == null)
			throw new NoSuchElementException();
		else
		{
			next = current;
			current = prev;
	        lookahead++;
	        while (true)
	        {
	        	prev = back();
	        	if (prev == null)
	        		break;
	        	if (--lookahead == -1)
	        		break;
	        }
			return current;
		}
	}

	public boolean hasNext() 
	{
		return next != null;
	}

	public T next() 
	{
		if (next == null)
			throw new NoSuchElementException();
		else
		{
			prev = current;
			current = next;
	        lookahead--;
	        while (true)
	        {
	        	next = advance();
	        	if (next == null)
	        		break;
	        	if (++lookahead == 1)
	        		break;
	        }
			return current;
		}
	}

	public void remove() 
	{
		throw new UnsupportedOperationException();
	}
	
	public boolean isOrdered()
	{
		return true;
	}
}