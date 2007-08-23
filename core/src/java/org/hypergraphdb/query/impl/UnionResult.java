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
 * Combines two result set into a result representing their set theoretic union. It
 * is assumed that the input sets are ordered and hence that the objects contained
 * therein are <code>java.lang.Comparable</code> instances.
 * </p>
 * 
 * <p>
 * The produced result set also has its elements in order.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class UnionResult implements HGSearchResult 
{
	//
	// The left and right result sets.
	//
	private HGSearchResult left, right;
	
	//
	// Indicates whether both input sets should be moved because
	// the last comparison of their "current" elements yielded an
	// equality.
	//
	private boolean move_both = true;
	
	//
	// The input set (left or right) that hold the current element
	// of the union. 
	//
	private HGSearchResult last_choice = null;
	
	//
	// IMPLEMENTATION NOTE: to understand how this works, one only
	// need to go through the logic of either 'next' or 'prev', as the
	// other is defined in completely analogous way, by symmetry.
	//
	
	private Object select(Comparable L, Comparable R)
	{
		int comp = L.compareTo(R);
		if (comp == 0)
		{
			last_choice = left;
			move_both = true;
			return L;
		}
		else if (comp < 0)
		{
			last_choice = left;
			move_both = false;
			return L;
		}
		else
		{
			last_choice = right;
			move_both = false;
			return R;
		}		
	}
	
	private Object selectBack(Comparable L, Comparable R)
	{
		int comp = L.compareTo(R);
		if (comp == 0)
		{
			last_choice = left;
			move_both = true;
			return L;
		}
		else if (comp < 0)
		{
			last_choice = right;
			move_both = false;
			return R;
		}
		else
		{
			last_choice = left;
			move_both = false;
			return L;
		}		
	}
	
	public UnionResult(HGSearchResult left, HGSearchResult right)
	{
		this.left = left;
		this.right = right;
	}
	
	public Object current() 
	{
		if (last_choice == null)
			throw new NoSuchElementException();
		else
			return last_choice.current();
	}

	public void close() 
	{
		left.close();
		right.close();
	}

	public Object prev() 
	{
		if (!hasPrev())
			throw new NoSuchElementException();
		
		if (move_both)
		{
			if (left.hasPrev())
				if (!right.hasPrev())
				{
					last_choice = left;
					move_both = false;					
					return left.prev();
				}
				else
					return selectBack((Comparable)left.prev(), (Comparable)right.prev());
			else
			{
				last_choice = right;
				move_both = false;				
				return right.prev();
			}
		}
		else if (last_choice == left)
		{
			if (!left.hasPrev())
			{
				last_choice = right;
				move_both = false;				
				return right.current();
			}
			else
				return selectBack((Comparable)left.prev(), (Comparable)right.current());
		}
		else if (last_choice == right)
		{
			if (!right.hasPrev())
			{
				last_choice = left;
				move_both = false;				
				return left.current();
			}
			else
				return selectBack((Comparable)left.current(), (Comparable)right.prev());
		}
		else
			throw new NoSuchElementException("This should never be thrown from here!!!"); // we'll never get here		
	}

	public boolean hasPrev() 
	{
		if (left.hasPrev())
			return true;
		else if (right.hasPrev())
			return true;
		else if (last_choice == null)
			return false;
		else if (last_choice == left)
			return ((Comparable)left.current()).compareTo(right.current()) > 0;
		else // last choice == right
			return ((Comparable)right.current()).compareTo(left.current()) > 0;	
	}
	
	public boolean hasNext() 
	{
		if (left.hasNext())
			return true;
		else if (right.hasNext())
			return true;
		else if (last_choice == null)
			return false;
		else if (last_choice == left)
			return ((Comparable)left.current()).compareTo(right.current()) < 0;
		else // last choice == right
			return ((Comparable)right.current()).compareTo(left.current()) < 0;			
	}

	public Object next() 
	{
		if (!hasNext())
			throw new NoSuchElementException();
		
		if (move_both)
		{
			if (left.hasNext())
				if (!right.hasNext())
				{
					last_choice = left;
					move_both = false;
					return left.next();
				}
				else
					return select((Comparable)left.next(),(Comparable)right.next());
			else
			{
				last_choice = right;
				move_both = false;
				return right.next();
			}
		}
		else if (last_choice == left)
		{
			if (!left.hasNext())
			{
				last_choice = right;
				move_both = false;
				return right.current();
			}
			else
				return select((Comparable)left.next(), (Comparable)right.current());
		}
		else if (last_choice == right)
		{
			if (!right.hasNext())
			{
				last_choice = left;
				move_both = false;
				return left.current();
			}
			else
				return select((Comparable)left.current(), (Comparable)right.next());
		}
		else
			throw new NoSuchElementException("This should never be thrown from here!!!"); // we'll never get here
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