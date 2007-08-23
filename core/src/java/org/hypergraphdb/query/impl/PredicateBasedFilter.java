/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
/*
 * Created on Aug 13, 2005
 *
 */
package org.hypergraphdb.query.impl;


import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.HGUtils;

/**
 * <p>
 * A <code>HGQuery</code> whose result is constructed by filtering the result set
 * of another <code>HGQuery</code> according to a <code>HGQueryCondition</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public final class PredicateBasedFilter extends HGQuery 
{
    private HyperGraph hg;
	private HGQuery query;
	private HGAtomPredicate predicate;
	
	/**
	 * <p>Construct a <code>PredicateBasedFilter</code>, filtering the result
	 * set of a given query based on a <code>HGQueryCondition</code>.
	 * </p>
	 * 
	 * @param query The base query that is being filtered.
	 * @param predicate The filtering predicate.
	 */
	public PredicateBasedFilter(HyperGraph hg, HGQuery query, HGAtomPredicate predicate)
	{
        this.hg = hg;
		this.query = query;
		this.predicate = predicate;
	}
	
	public HGSearchResult execute() 
	{
		HGSearchResult baseResult = query.execute();
		try
		{
			while (baseResult.hasNext())
			{
				Object next = baseResult.next();
				if (predicate.satisfies(hg, (HGHandle)next))
					return new FilteredResultSet(hg, baseResult, predicate, 1);
			}
		}
		catch (Throwable t)
		{
			HGUtils.closeNoException(baseResult);
			HGUtils.wrapAndRethrow(t);			
		}
		baseResult.close();		
		return HGSearchResult.EMPTY;
	}
}