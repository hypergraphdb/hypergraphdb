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

import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.util.Mapping;

/**
 * <p>
 * A <code>HGQuery</code> that transforms the result of an underlying query
 * by applying a provided mapping.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class ResultMapQuery extends HGQuery implements Iterable<HGQuery> 
{
	private Mapping mapping = null;
	private HGQuery query = null;
	
	/**
	 * <p>Create a query that transforms the output of a given query by
	 * the specified mapping. You can pass <code>null</code> as the 
	 * <code>mapping</code> for the identity mapping.</p>
	 */
	public ResultMapQuery(HGQuery query, Mapping mapping)
	{
		this.query = query;
		this.mapping = mapping;
	}
	
	public void setMapping(Mapping mapping)
	{
		this.mapping = mapping;
	}
	
	public Mapping getMapping()
	{
		return mapping;
	}
	
	public HGSearchResult execute() 
	{
		if (query == null)
			throw new NullPointerException("null query in ResultMapQuery.");
		else if (mapping == null)
			return query.execute();
		else
			return new MappedResult(query.execute(), mapping);
	} 

	public Iterator<HGQuery> iterator()
    {
        return Collections.singleton(this.query).iterator();
    }
}