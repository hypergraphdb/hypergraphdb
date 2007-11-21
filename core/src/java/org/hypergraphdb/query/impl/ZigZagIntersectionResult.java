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

	// Here, we try to make the current position of this zig-zag result
	// set be the current position of the left_or_right parameter (which is
	// one of the 'left' or 'right' result sets). If we succeed, we return
	// true, otherwise we return false. 
	private boolean positionTo(HGRandomAccessResult left_or_right)
	{
		if (left != left_or_right)
			swap();
		prev = back();
		current = advance();
		if (current == null)
			return false;
		next = advance();
		return true;
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
		Object save_left = left.current();		
		GotoResult r_l = left.goTo(value, exactMatch);
		if (r_l == GotoResult.nothing)
			return GotoResult.nothing;
		
		Object save_right = right.current();
		GotoResult r_r = right.goTo(value, exactMatch);
		if (r_r == GotoResult.nothing)
		{
			left.goTo(save_left, true); // restore current position of left...
			return GotoResult.nothing;
		}
		
		if (r_l == GotoResult.found)
		{
			if (r_r == GotoResult.found)
			{
				current = left.current();
				prev = back();
				if (prev != null) advance();
				next = advance();
				return GotoResult.found;
			}
			else // r_r == close
			{
				if (positionTo(right))
					return GotoResult.close;
				else
				{
					left.goTo(save_left, true);
					right.goTo(save_right, true);
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
					left.goTo(save_left, true);
					right.goTo(save_right, true);
					return GotoResult.nothing;
				}
			}
			else
			{
				boolean moved;
				if (((Comparable)left.current()).compareTo(right.current()) > 0)
					moved = positionTo(left);
				else
					moved = positionTo(right);
				if (moved)
					return GotoResult.close;
				else
				{
					left.goTo(save_left, true);
					right.goTo(save_right, true);
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