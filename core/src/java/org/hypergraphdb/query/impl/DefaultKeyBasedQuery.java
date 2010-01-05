/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.ValueSetter;

public class DefaultKeyBasedQuery<Key, Value> extends KeyBasedQuery<Key, Value> 
{
	private HyperGraph graph;
	private HGQueryCondition cond;
	private ValueSetter<Key> setter;
	private Key currentKey;
	
	public DefaultKeyBasedQuery(HyperGraph graph, HGQueryCondition cond, ValueSetter<Key> setter)
	{
		this.graph = graph;
		this.cond = cond;
		this.setter = setter;
	}
	
	@Override
	public Key getKey() 
	{
		return currentKey;
	}

	@Override
	public void setKey(Key key) 
	{
	    this.currentKey = key;
		setter.set(key);
	}

	@Override
	public HGSearchResult<Value> execute() 
	{
	    if (currentKey == null)
	        throw new NullPointerException("Key value not set.");
		return graph.find(cond);
	}
}
