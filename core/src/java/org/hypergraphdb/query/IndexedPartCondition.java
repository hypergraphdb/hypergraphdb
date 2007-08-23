package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.util.HGUtils;

public class IndexedPartCondition implements HGQueryCondition
{
	private HGIndex idx;
	private Object partValue;
	private HGHandle type;
	private ComparisonOperator operator;
	
	public IndexedPartCondition(HGHandle type, HGIndex idx, Object partValue, ComparisonOperator operator)
	{
		this.type = type;
		this.idx = idx;
		this.partValue = partValue;
		this.operator = operator;
	}

	public HGIndex getIndex()
	{
		return idx;
	}

	public Object getPartValue()
	{
		return partValue;
	}

	public HGHandle getType()
	{
		return type;
	}

	public ComparisonOperator getOperator()
	{
		return operator;
	}
	
	public int hashCode()
	{
		return HGUtils.hashThem(type, idx);
	}
	
	public boolean equals(Object other)
	{
		if (! (other instanceof IndexedPartCondition))
			return false;
		IndexedPartCondition ip = (IndexedPartCondition)other;
		return HGUtils.eq(idx, ip.idx) && HGUtils.eq(operator, ip.operator);
	}
}