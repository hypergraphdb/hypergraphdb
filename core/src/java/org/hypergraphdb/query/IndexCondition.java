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

	public IndexCondition()
	{		
	}
	
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

	public void setIndex(HGIndex<Key,Value> idx)
	{
		this.idx = idx;
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

	@Override
	public int hashCode()
	{
		final int prime = 31;
		int result = 1;
		result = prime * result + ((idx == null) ? 0 : idx.getName().hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj)
	{
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IndexCondition other = (IndexCondition) obj;
		if (idx == null)
		{
			if (other.idx != null)
				return false;
		}
		else if (!idx.getName().equals(other.idx.getName()))
			return false;
		if (key == null)
		{
			if (other.key != null)
				return false;
		}
		else if (!key.equals(other.key))
			return false;
		if (operator != other.operator)
			return false;
		return true;
	}
	
}
