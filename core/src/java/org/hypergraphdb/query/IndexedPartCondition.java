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
import org.hypergraphdb.util.HGUtils;

public class IndexedPartCondition implements HGQueryCondition
{
	private HGIndex<?, ?> idx;
	private Object partValue;
	private HGHandle type;
	private ComparisonOperator operator;
	
	public IndexedPartCondition(HGHandle type, 
								HGIndex<?, ?> idx, 
								Object partValue, 
								ComparisonOperator operator)
	{
		this.type = type;
		this.idx = idx;
		this.partValue = partValue;
		this.operator = operator;
	}

	public HGIndex<?, ?> getIndex()
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
		return HGUtils.eq(idx, ip.idx) && 
			   HGUtils.eq(operator, ip.operator) &&
			   HGUtils.eq(type, ip.type) &&
			   HGUtils.eq(partValue, ip.partValue);
	}
}
