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
public class UnionResult<T> implements HGSearchResult<T> 
{
	//
	// The left and right result sets.
	//
	private HGSearchResult<T> left, right;
	private boolean leftBOF = false, rightBOF = false, leftEOF=false, rightEOF=false;
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
	private HGSearchResult<T> last_choice = null;
	
	//
	// IMPLEMENTATION NOTE: to understand how this works, one only
	// need to go through the logic of either 'next' or 'prev', as the
	// other is defined in completely analogous way, by symmetry.
	//
	
	private T select(Comparable<T> L, T R)
	{
		int comp = L.compareTo(R);
		if (comp == 0)
		{
			last_choice = left;
			move_both = true;
			return (T)L;
		}
		else if (comp < 0)
		{
			last_choice = left;
			move_both = false;
			return (T)L;
		}
		else
		{
			last_choice = right;
			move_both = false;
			return R;
		}		
	}
	
	private T selectBack(Comparable<T> L, T R)
	{
		int comp = L.compareTo(R);
		if (comp == 0)
		{
			last_choice = left;
			move_both = true;
			return (T)L;
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
			return (T)L;
		}		
	}
	
	public UnionResult(HGSearchResult left, HGSearchResult right)
	{
		this.left = left;
		this.right = right;
	}
	
	public T current() 
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

	public T prev() 
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
					rightBOF = true;
					return left.prev();
				}
				else
					return selectBack((Comparable<T>)left.prev(), right.prev());
			else
			{
				last_choice = right;
				move_both = false;	
				leftBOF = true;
				return right.prev();
			}
		}
		else if (last_choice == left)
		{
			if (!left.hasPrev())
			{
				last_choice = right;
				move_both = false;
				leftBOF = true;
				return right.current();
			}
			else
				if (rightBOF) return left.prev();
				else return selectBack((Comparable<T>)left.prev(), right.current());
		}
		else if (last_choice == right)
		{
			if (!right.hasPrev())
			{
				last_choice = left;
				move_both = false;
				rightBOF = true;
				return left.current();
			}
			else
				if (leftBOF) return right.prev();
				else return selectBack((Comparable<T>)left.current(), right.prev());
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
		else if (last_choice == left && !rightBOF)
			return ((Comparable)left.current()).compareTo(right.current()) > 0;
		else if (last_choice == right && !leftBOF)
			return ((Comparable)right.current()).compareTo(left.current()) > 0;
		else return false;
	}
	
	public boolean hasNext() 
	{
		if (left.hasNext())
			return true;
		else if (right.hasNext())
			return true;
		else if (last_choice == null)
			return false;
		else if (last_choice == left && !rightEOF)
			return ((Comparable)left.current()).compareTo(right.current()) < 0;
		else if (last_choice == right && !leftEOF)
			return ((Comparable)right.current()).compareTo(left.current()) < 0;			
		else return false;
	}

	public T next() 
	{
		if (!hasNext())
			throw new NoSuchElementException();
		
		if (move_both)
		{
			if (left.hasNext())
			{
				if (!right.hasNext())
				{
					last_choice = left;
					move_both = false;
					rightEOF = true;
					return left.next();
				}
				else
					return select((Comparable<T>)left.next(),right.next());
			}
			else
			{
				last_choice = right;
				move_both = false;
				leftEOF = true;
				return right.next();
			}
		}
		else if (last_choice == left)
		{
			if (!left.hasNext())
			{
				last_choice = right;
				move_both = false;
				leftEOF = true;
				return right.current();
			}
			else if (rightEOF) 
			    return left.next();
			else 
			    return select((Comparable<T>)left.next(), right.current());
		}
		else if (last_choice == right)
		{
			if (!right.hasNext())
			{
				last_choice = left;
				move_both = false;
				rightEOF = true;
				return left.current();
			}
			else if (leftEOF) 
			    return right.next();
			else 
			    return select((Comparable<T>)left.current(), right.next());
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
