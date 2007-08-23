package org.hypergraphdb.query.impl;

import java.util.NoSuchElementException;
import org.hypergraphdb.HGRandomAccessResult;

/**
 * <p>
 * The <code>ZigZagIntersectionResult</code> operates on two sorted, random access
 * result sets.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class ZigZagIntersectionResult implements HGRandomAccessResult, RSCombiner<HGRandomAccessResult, HGRandomAccessResult>
{
	private HGRandomAccessResult left, right;
	private Object current = null, next = null, prev = null;

	private void swap()
	{
		HGRandomAccessResult tmp = left;
		left = right;
		right = tmp;
	}
	
	private Object advance()
	{
		while (true)
		{
			if (!left.hasNext() || !right.hasNext())
				return null;
			Object x = left.next();
			if (right.goTo(x, false))
				return x;
			else
			{
				if (right.hasPrev())
					right.prev();
				swap();
			}
		}
	}
	
	private Object back()
	{
		while (true)
		{
			if (!left.hasPrev() || !right.hasPrev())
				return null;
			Object x = left.prev();
			if (right.goTo(x, false))
				return x;
			else
				swap();
		}
	}
	
	public ZigZagIntersectionResult()
	{		
	}
	
	public ZigZagIntersectionResult(HGRandomAccessResult left, HGRandomAccessResult right)
	{
		init(left, right);
	}

	public void init(HGRandomAccessResult left, HGRandomAccessResult right)
	{
		this.left = left;
		this.right = right;
		next = advance();
	}
	
	public boolean goTo(Object value, boolean exactMatch) 
	{
		return left.goTo(value, exactMatch) && right.goTo(value, exactMatch);
	}

	public void close() 
	{
		left.close();
		right.close();		
	}

	public Object current() 
	{
		if (current == null)
			throw new NoSuchElementException();
		else
			return current;
	}

	public boolean isOrdered() 
	{
		return true;
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
}