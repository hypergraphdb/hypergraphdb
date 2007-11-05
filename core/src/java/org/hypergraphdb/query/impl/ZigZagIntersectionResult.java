package org.hypergraphdb.query.impl;

import java.util.NoSuchElementException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.util.HGUtils;

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
		boolean use_next = true;
		while (true)
		{
			if (!left.hasNext() && use_next || !right.hasNext())
				return null;
			Object x;
			if (use_next)
			{
				x = left.next();
			}
			else
			{
				x = left.current();
				use_next = true;
			}
			switch (right.goTo(x, false))
			{
				case found: 
				{
					return x;
				}
				case close:
				{
					if (right.hasPrev())
						right.prev();
					else
						use_next = false; // this happens when we've moved to the 1st element of the set.
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
	
	private Object back()
	{
		while (true)
		{
			if (!left.hasPrev() || !right.hasPrev())
				return null;
			Object x = left.prev();
			if (right.goTo(x, false) == GotoResult.found)
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
	
	public GotoResult goTo(Object value, boolean exactMatch) 
	{
		GotoResult r_l = left.goTo(value, exactMatch);
		GotoResult r_r = right.goTo(value, exactMatch); 
		if (r_l == GotoResult.found)
		{
			if (r_r == GotoResult.found)
				return GotoResult.found;
			else
				return GotoResult.close;
		}
		else if (r_l == GotoResult.close)
			return GotoResult.close;
		else
		{
			if (r_r == GotoResult.nothing)
				return GotoResult.nothing;
			else
				return GotoResult.close;
		}
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