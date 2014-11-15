/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

public class IndexedPartCondition implements HGQueryCondition
{
	private HGIndex<?, ?> idx;
	private Ref<Object> partValue;
	private HGHandle type;
	private ComparisonOperator operator;

	public IndexedPartCondition()
	{		
	}
	
	public IndexedPartCondition(HGHandle type, 
			HGIndex<?, ?> idx, 
			Object partValue, 
			ComparisonOperator operator)
	{
		this(type, idx, hg.constant(partValue), operator);
	}
	
	public IndexedPartCondition(HGHandle type, 
								HGIndex<?, ?> idx, 
								Ref<Object> partValue, 
								ComparisonOperator operator)
	{
		this.type = type;
		this.idx = idx;
		this.partValue = partValue;
		this.operator = operator;
	}

	public void setIndex(HGIndex<?,?> idx)
	{
		this.idx = idx;
	}
	
	public HGIndex<?, ?> getIndex()
	{
		return idx;
	}

	public Ref<Object> getPartValueReference()
	{
		return partValue;
	}
	
	public void setPartValueReference(Ref<Object> partValue)
	{
		this.partValue = partValue;
	}
	
	public Object getPartValue()
	{
		return partValue;
	}

	public void setType(HGHandle type)
	{
		this.type = type;
	}
	
	public HGHandle getType()
	{
		return type;
	}

	public void setOperator(ComparisonOperator operator)
	{
		this.operator = operator;
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
		return HGUtils.eq(idx.getName(), ip.idx.getName()) && 
			   HGUtils.eq(operator, ip.operator) &&
			   HGUtils.eq(type, ip.type) &&
			   HGUtils.eq(partValue, ip.partValue);
	}
}
