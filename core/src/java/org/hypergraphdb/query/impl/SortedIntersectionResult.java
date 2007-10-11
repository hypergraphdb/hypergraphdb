/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
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
public class SortedIntersectionResult implements HGSearchResult, RSCombiner<HGSearchResult, HGSearchResult>
{
	private HGSearchResult left, right;
	private Object current = null, next = null, prev = null;
	
	private Object advance()
	{
		boolean advance_left = true, advance_right = true;
		Comparable lnext = null, rnext = null;
		while (true)				
		{
			if (advance_left)
				if (!left.hasNext())
					return null;
				else
					lnext = (Comparable)left.next();
			if (advance_right)
				if (!right.hasNext())
					return null;
				else
					rnext = (Comparable)right.next();
			int comp = lnext.compareTo(rnext);
			if (comp == 0)
				return lnext;
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
	
	private Object back()
	{
		boolean back_left = true, back_right = true;
		Comparable lprev = null, rprev = null;
		while (true)				
		{
			if (back_left)
				if (!left.hasPrev())
					return null;
				else
					lprev = (Comparable)left.prev();
			if (back_right)
				if (!right.hasPrev())
					return null;
				else
					rprev = (Comparable)right.prev();
			int comp = lprev.compareTo(rprev);
			if (comp == 0)
				return lprev;
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
	
	public SortedIntersectionResult()
	{		
	}
	
	public SortedIntersectionResult(HGSearchResult left, HGSearchResult right)
	{
		init(left, right);
	}
	
	public void init(HGSearchResult left, HGSearchResult right)
	{
		this.left = left;
		this.right = right;
		next = advance();
	}
	
	public Object current() 
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

	public Object prev() 
	{
		if (prev == null)
			throw new NoSuchElementException();
		else
		{
			next = current;
			current = prev;
			prev = back();
			return current;
		}
	}

	public boolean hasNext() 
	{
		return next != null;
	}

	public Object next() 
	{
		if (next == null)
			throw new NoSuchElementException();
		else
		{
			prev = current;
			current = next;
			next = advance();
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