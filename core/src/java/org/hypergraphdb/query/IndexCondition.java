/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.Ref;

public class IndexCondition<Key,Value> implements HGQueryCondition
{
	private HGIndex<Key,Value> idx;
	private Ref<Key> key = null;
	private ComparisonOperator operator = ComparisonOperator.EQ;

	public IndexCondition(HGIndex<Key,Value> idx, Key key)
	{
		this.idx = idx;
		this.key = hg.constant(key);
	}
	
	public IndexCondition(HGIndex<Key,Value> idx, Key key, ComparisonOperator op)
	{
		this.idx = idx;
		this.key = hg.constant(key);
		this.operator = op;
	}	
	
	public IndexCondition(HGIndex<Key,Value> idx, Ref<Key> key, ComparisonOperator op)
	{
	    this.idx = idx;
	    this.key = key;
	    this.operator = op;
	}
	
	public HGIndex<Key,Value> getIndex()
	{
		return idx;
	}
	
	public Object getKey()
	{
		return key.get();
	}
	
	public Ref<Key> getKeyReference()
	{
	    return key;
	}
	
	public void setKeyReference(Ref<Key> key)
	{
	    this.key = key;
	}
	
	public ComparisonOperator getOperator()
	{
		return operator;
	}
}
