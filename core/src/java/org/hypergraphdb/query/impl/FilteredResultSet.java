package org.hypergraphdb.query.impl;

import java.util.NoSuchElementException;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.CloseMe;

/**
 * 
 * <p>
 * Filter a result set through a predicate. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class FilteredResultSet implements HGSearchResult
{
    private HyperGraph hg;
	private HGSearchResult searchResult;
	private HGAtomPredicate predicate;		
	private Object current = null;

	//
	// The number of elements preceeding the current in the underlying
	// searchResult that satisfy the predicate.
	//
	private int prevCount = 0;
	
	//
	// The diff in the underlying result b/w the location of our 'current' member
	// variable and its own "current" element. 
	// 
	private int lookahead = 0; 
	
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
	public FilteredResultSet(HyperGraph hg, 
							 HGSearchResult searchResult, 
							 HGAtomPredicate predicate,
							 int lookahead)
	{
        this.hg = hg;
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

	public Object current() 
	{
		return current;
	}
	
	public boolean hasPrev() 
	{
		return prevCount > 0;
	}

	public Object prev() 
	{
		if (prevCount == 0)
			throw new NoSuchElementException();
		while (lookahead > 0)
		{
			lookahead--;
			searchResult.prev();
		}
		prevCount--;			
		while (searchResult.hasPrev() && !predicate.satisfies(hg, (HGHandle)searchResult.prev()));
		return current = searchResult.current();
	}

	public boolean hasNext() 
	{
		if (lookahead > 0)
			return predicate.satisfies(hg, (HGHandle)searchResult.current());
		
		while (true)
		{
			if (!searchResult.hasNext())
				return false;
			lookahead++;				
			if (!predicate.satisfies(hg, (HGHandle)searchResult.next()))
				continue;
			else
				return true;
		} 			
	}

	public Object next() 
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