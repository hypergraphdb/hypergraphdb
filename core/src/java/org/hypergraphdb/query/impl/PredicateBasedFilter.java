/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;


import java.util.Collections;
import java.util.Iterator;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * A <code>HGQuery</code> whose result is constructed by filtering the result set
 * of another <code>HGQuery</code> according to a <code>HGQueryCondition</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public final class PredicateBasedFilter<T> extends HGQuery<T> implements Iterable<HGQuery<T>>
{
	private HGQuery<T> query;
	private Mapping<T, Boolean> predicate;
	
	/**
	 * <p>Construct a <code>PredicateBasedFilter</code>, filtering the result
	 * set of a given query based on a <code>HGQueryCondition</code>.
	 * </p>
	 * 
	 * @param query The base query that is being filtered.
	 * @param predicate The filtering predicate.
	 */
	public PredicateBasedFilter(final HyperGraph graph, 
	                            final HGQuery<T> query, 
	                            final HGAtomPredicate atomPredicate)
	{
		this.graph = graph;
		this.query = query;
		this.predicate = new Mapping<T, Boolean>() {
		    public Boolean eval(T h)
		    {
		        return atomPredicate.satisfies(graph, (HGHandle)h);
		    }
		};
	}

  public PredicateBasedFilter(final HyperGraph graph, 
                              final HGQuery<T> query, 
                              final Mapping<T, Boolean> predicate)
  {
      this.graph = graph;
      this.query = query;
      this.predicate = predicate;
  }
    
	@SuppressWarnings("unchecked")
    public HGSearchResult<T> execute() 
	{
		HGSearchResult<T> baseResult = query.execute();
		try
		{
			while (baseResult.hasNext())
			{
				T next = baseResult.next();
				if (predicate.eval(next))
					return new FilteredResultSet<T>(baseResult, predicate, 1);
			}
		}
		catch (Throwable t)
		{
			HGUtils.closeNoException(baseResult);
			HGUtils.wrapAndRethrow(t);			
		}
		baseResult.close();		
		return (HGSearchResult<T>)HGSearchResult.EMPTY;
	}
	
	public HGQuery<T> getQuery()
	{
	    return query;
	}
	
	public Iterator<HGQuery<T>> iterator()
	{
	    return Collections.singleton(this.query).iterator();
	}
}