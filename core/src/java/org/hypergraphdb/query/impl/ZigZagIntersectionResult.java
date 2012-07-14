/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import java.util.NoSuchElementException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;

/**
 * <p>
 * The <code>ZigZagIntersectionResult</code> operates on two sorted, random access
 * result sets.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class ZigZagIntersectionResult<T> implements HGRandomAccessResult<T> // , RSCombiner<T>
{
	private static final Object UNKNOWN = new Object();
	
	private HGRandomAccessResult<T> left, right;
	private Object current = UNKNOWN, next = UNKNOWN, prev = UNKNOWN;
	
	private void swap()
	{
		HGRandomAccessResult<T> tmp = left;
		left = right;
		right = tmp;
	}
	
	private T advance()
	{
		boolean use_next = true;
		while (true)
		{
			if (!left.hasNext() && use_next || !right.hasNext())
				return null;
			T x;
			if (use_next)
			{
				x = left.next();
			}
			else
			{
				x = left.current();
				use_next = true;
			}
//			System.out.println("at " + x);
			switch (right.goTo(x, false))
			{
				case found: 
				{
					return x;
				}
				case close:
				{
					use_next = false;
					swap();		
					break;
				}
				default:
				{
					// this means that all we have reached EOF since 'x', the
					// element on the left is greater than all elements on the right
					return null;
				}
			}
		}
	}
	
	private T back()
	{
		T result = null;
		while (true)
		{
			if (!left.hasPrev() || !right.hasPrev())
				break;
			T x = left.prev();
			if (right.goTo(x, false) == GotoResult.found)
			{ 
				result = x; 
				break; 
			}
			else
				swap();
		}
		return result;
	}

	// Here, we try to make the current position of this zig-zag result
	// set be the current position of the left_or_right parameter or the closest
	// position following that current. If we succeed, we return
	// true, otherwise we return false. 
	private boolean positionTo(HGRandomAccessResult<T> left_or_right)
	{
		// Make sure the argument 'left_or_right' is the left rs in the loop below
		if (left != left_or_right) 
			swap();
		while (true)
		{
			switch (right.goTo(left.current(), false))
			{
				case found: 
				{
					current = left.current();
					next = prev = UNKNOWN;
					return true;
				}
				case close:
				{					 
					swap();		
					break;
				}
				default:
				{
					return false;
				}				
			}
		}
	}
		
	public final static class Combiner<T> implements RSCombiner<T>
	{
		public HGSearchResult<T> combine(HGSearchResult<T> left, HGSearchResult<T> right)
		{
			return new ZigZagIntersectionResult<T>((HGRandomAccessResult<T>)left, (HGRandomAccessResult<T>)right);
		}
	}
	
//	public ZigZagIntersectionResult()
//	{		
//	}
	
	public ZigZagIntersectionResult(HGRandomAccessResult<T> left, HGRandomAccessResult<T> right)
	{
		this.left = left;
		this.right = right;
	}

//	public void init(HGSearchResult<T> left, HGSearchResult<T> right)
//	{
//		this.left = (HGRandomAccessResult<T>)left;
//		this.right = (HGRandomAccessResult<T>)right;
//	}
	
//	public void reset() {
//		current = UNKNOWN;
//		next = UNKNOWN;
//		prev = UNKNOWN;
//	}

	public void goBeforeFirst()
	{
	    left.goBeforeFirst();
	    right.goBeforeFirst();
	}
	
	public void goAfterLast()
	{
	    left.goAfterLast();
	    right.goAfterLast();
	}
	
	public GotoResult goTo(T value, boolean exactMatch) 
	{
		// We need to save state of both left and right cursor. Because of swapping, save the current
		// left and the current right in local variables.
		HGRandomAccessResult<T> starting_left = left, starting_right = right; 
		T save_left = null;
		T save_right = null;
		try
		{
			save_left = left.current();		
			save_right = right.current();
		}
		catch (NoSuchElementException ex)
		{
			// it means that one of them is empty since we always advance them both in 'init'
			return GotoResult.nothing;
		}
		
		GotoResult r_l = left.goTo(value, exactMatch);
		if (r_l == GotoResult.nothing)
			return GotoResult.nothing;
		
		
		GotoResult r_r = right.goTo(value, exactMatch);
		if (r_r == GotoResult.nothing)
		{
			starting_left.goTo(save_left, true); // restore current position of left...
			return GotoResult.nothing;
		}
		
		if (r_l == GotoResult.found)
		{
			if (r_r == GotoResult.found)
			{
				current = left.current();
				next = prev = UNKNOWN;
				return GotoResult.found;
			}
			else // r_r == close
			{
				if (positionTo(right))
					return GotoResult.close;
				else
				{
					starting_left.goTo(save_left, true);
					starting_right.goTo(save_right, true);
					return GotoResult.nothing;
				}			
			}
		}
		else // r_l == close
		{
			if (r_r == GotoResult.found)
			{
				if (positionTo(left))
					return GotoResult.close;
				else
				{
					starting_left.goTo(save_left, true);
					starting_right.goTo(save_right, true);
					return GotoResult.nothing;
				}
			}
			else // both are 'close'
			{
				int cmp = ((Comparable<T>)left.current()).compareTo(right.current());
				if (cmp == 0 && positionTo(left) || cmp > 0 && positionTo(left) || positionTo(right))
					return GotoResult.close;
				else
				{
					starting_left.goTo(save_left, true);
					starting_right.goTo(save_right, true);
					return GotoResult.nothing;					
				}				
			}		
		}	
	}

	public void close() 
	{
		left.close();
		right.close();		
	}

	public T current() 
	{
		if (current == UNKNOWN)
			throw new NoSuchElementException();
		else
			return (T)current;
	}

	public boolean isOrdered() 
	{
		return true;
	}

	public boolean hasPrev() 
	{
		if (prev == UNKNOWN)
			prev = back();
		return prev != null;
	}

	public T prev() 
	{
		if (!hasPrev())
			throw new NoSuchElementException();
		else
		{
			next = current;
			current = prev;
			prev = UNKNOWN;
			return (T)current;
		}
	}

	public boolean hasNext() 
	{
		if (next == UNKNOWN)
			next = advance();
		return next != null;
	}

	public T next() 
	{
		if (!hasNext())
			throw new NoSuchElementException();
		else
		{
			prev = current;
			current = next;
			next = UNKNOWN;
			return (T)current;
		}
	}

	public void remove() 
	{
		throw new UnsupportedOperationException();		
	}	
}
