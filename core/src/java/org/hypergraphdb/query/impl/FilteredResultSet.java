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
import org.hypergraphdb.util.CloseMe;
import org.hypergraphdb.util.Mapping;

/**
 * 
 * <p>
 * Filter a result set through a predicate. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class FilteredResultSet<T> implements HGSearchResult<T>
{
	HGSearchResult<T> searchResult;
	Mapping<T, Boolean> predicate;		
	T current = null;

	//
	// The number of elements preceding the current in the underlying
	// searchResult that satisfy the predicate.
	//
	int prevCount = -1;
	
	//
	// The diff in the underlying result b/w the location of our 'current' member
	// variable and its own "current" element. 
	// 
	int lookahead = 0; 
	
	protected FilteredResultSet() {}
	
	/**
	 * <p>
	 * The constructor assumes the underlying set is already positioned to the
	 * first matching entity.
	 * </p>
	 * 
	 * @param hg The 
	 * @param searchResult
	 * @param predicate
	 */
	public FilteredResultSet(HGSearchResult<T> searchResult, 
							 Mapping<T, Boolean> predicate,
							 int lookahead)
	{
		this.searchResult = searchResult;
		this.predicate = predicate;
		this.lookahead = lookahead;
	}
	
	public void close() 
	{
		searchResult.close();
		if (predicate instanceof CloseMe)
			((CloseMe)predicate).close();
	}

	public T current() 
	{
		return current;
	}
	
	public boolean hasPrev() 
	{
		return prevCount > 0;
	}

	public T prev() 
	{
		if (prevCount == 0)
			throw new NoSuchElementException();
		while (lookahead > 0)
		{
			lookahead--;
			searchResult.prev();
		}
		prevCount--;			
		while (searchResult.hasPrev() && !predicate.eval(searchResult.prev()));
		return current = searchResult.current();
	}

	public boolean hasNext() 
	{
		if (lookahead > 0) {
//			System.out.println("Evaluating with LA current:" + searchResult.current());
			boolean res = predicate.eval(searchResult.current()); 
			if (res) {
//				System.out.println("Evaluating with LA current was valid");
				return true;
			}
//			System.out.println("Evaluating with LA current was invalid, so trying further");
		}
		
		while (true)
		{
			if (!searchResult.hasNext()) {
//				System.out.println("No more next");
				return false;
			}
			lookahead++;				

			T next2 = searchResult.next();
//			System.out.println("Evaluating with for next:" + next2);
			
			if (!predicate.eval(next2))
				continue;
			else {
//				System.out.println("Has next is true for next of:" + next2);
				return true;
			}
		} 			
	}

	public T next() 
	{
		if (!hasNext())
			throw new NoSuchElementException();
		else
		{
			prevCount++;
			lookahead = 0;
			return current = searchResult.current();
		}
	}
	
	public void remove() 
	{
		while (lookahead > 0)
		{
			searchResult.prev();
			lookahead--;
		}
		searchResult.remove();
	}
	
	public boolean isOrdered()
	{
		return searchResult.isOrdered();
	}
}
