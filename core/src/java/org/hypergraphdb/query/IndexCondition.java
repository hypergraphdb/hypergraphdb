package org.hypergraphdb.query;

import org.hypergraphdb.HGIndex;

public class IndexCondition<Key,Value> implements HGQueryCondition
{
	private HGIndex<Key,Value> idx;
	private Key key = null;
	private ComparisonOperator operator = ComparisonOperator.EQ;
	
/*
 * 
 * an 'idx' only constructor would yield a query that scans an index (either its keys
 * or its values), but we haven't had a use case for that yet.
	public IndexCondition(HGIndex<?, ?> idx)
	{
		this.idx = idx;
	}
	*/
	
	public IndexCondition(HGIndex<Key,Value> idx, Key key)
	{
		this.idx = idx;
		this.key = key;
	}
	
	public IndexCondition(HGIndex<Key,Value> idx, Key key, ComparisonOperator op)
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
		return key;
	}
	
	public ComparisonOperator getOperator()
	{
		return operator;
	}
}