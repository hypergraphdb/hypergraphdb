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
 * 
 * <p>
 * A piped query result takes the output of a query, in the form of 
 * a <code>HGSearchResult</code> instance and uses it as input to a "pipe"
 * query. 
 * </p>
 * 
 * <p>
 * Such a result may be used to implement various query operators (e.g. union,
 * intersection) when one of the operands is an indexed set and can be queried
 * directly by a key.
 * </p>
 * @author Borislav Iordanov
 */
public class PipedResult<Key, Value> implements HGSearchResult<Value> 
{
	private HGSearchResult<Key> in;
	private KeyBasedQuery<Key, Value> pipe;
	private HGSearchResult<Value> currentPiped = null, 
						          nextPiped = null, 
						          previousPiped = null;	
	private boolean own_in;
	
	/**
	 * 
	 * @param in The input query result, can't be <code>null</code>.
	 * @param pipe The pipe query, can't be <code>null</code>.
	 * @param own_in Specifies whether to close the input result object when this object is closed.
	 */
	public PipedResult(HGSearchResult<Key> in, KeyBasedQuery<Key, Value> pipe, boolean own_in)
	{
		this.in = in;
		this.pipe = pipe;
		this.own_in = own_in;
		
		if (in.hasNext())
		{
			pipe.setKey(in.next());
			currentPiped = pipe.execute();
		}
	}
	
	public Value current() 
	{
		if (currentPiped == null)
			throw new NoSuchElementException();
		else
			return currentPiped.current();
	}

	public void close() 
	{
		if (currentPiped != null)
		{
			currentPiped.close();
			currentPiped = null;
		}
		if (nextPiped != null)
		{
			nextPiped.close();
			nextPiped = null;
		}
		if (previousPiped != null)
		{
			previousPiped.close();
			previousPiped = null;
		}
		
		if (own_in && in != null )
			in.close();
	}

	public boolean hasPrev() 
	{
		if (currentPiped == null)
			return false;
		else if (currentPiped.hasPrev())
			return true;
		else if (previousPiped != null)
			return previousPiped.hasPrev();
		else if (!in.hasPrev())
			return false;
		else while (true)
		{
			// TODO - This is not the optimal way to do things,
			// we're relying on the fact that an application will
			// not be scanning back and forth frentically, but that it
			// will only occasionally need to look to a prev element.
			// The problem is that when we execute the pipe query, we need
			// to scan to the end of the result set so as to maintain linearity
			// in the overall "PipedResult".
			// A more intelligent buffering of previous result sets need
			// to be implemented if this proves to be a bottleneck (e.g.
			// have a static variable for buffer size and grow it as the need
			// arises). Or perhaps we should simply forbid bidirectionality in those
			// results simply because it cannot be implemented efficiently....

			pipe.setKey(in.prev());
			previousPiped = pipe.execute();
			if (previousPiped.hasNext())
			{
				do { previousPiped.next(); } while (previousPiped.hasNext());
				return true;
			}
			else
			{
				previousPiped.close();
				previousPiped = null;
				if (!in.hasPrev())
					return false;
			}
		}
	}

	public Value prev() 
	{
		if (!hasPrev())
			throw new NoSuchElementException();
		else if (currentPiped.hasPrev())
			return currentPiped.prev();
		else
		{
			if (nextPiped != null)
				nextPiped.close();
			nextPiped = currentPiped;
			currentPiped = previousPiped;
			previousPiped = null;
			return currentPiped.prev();
		}
	}

	public void remove() 
	{
		throw new UnsupportedOperationException("PipedResult.remove");
	}

	public boolean hasNext() 
	{
		if (currentPiped == null)
			return false;
		else if (currentPiped.hasNext())
			return true;
		else if (nextPiped != null)
			return nextPiped.hasNext();
		else if (!in.hasNext())
			return false;
		else while (true)
		{
			pipe.setKey(in.next());
			nextPiped = pipe.execute();
			if (nextPiped.hasNext())
				return true;
			else
			{
				nextPiped.close();
				nextPiped = null;
				if (!in.hasNext())
					return false;
			}
		}
	}

	public Value next() 
	{
		if (!hasNext())
			throw new NoSuchElementException();
		if (currentPiped.hasNext())
			return currentPiped.next();
		else
		{
			if (previousPiped != null)
				previousPiped.close();
			previousPiped = currentPiped;
			currentPiped = nextPiped;
			nextPiped = null;
			return currentPiped.next();
		}
	}
	
	public boolean isOrdered()
	{
		return false;
	}
}
