package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

public class MapCondition implements HGQueryCondition
{
	private HGQueryCondition cond;
	private Mapping<?, ?> mapping;
	
	public MapCondition(HGQueryCondition condition, Mapping<?, ?> mapping)
	{
		this.cond = condition;
		this.mapping = mapping;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle handle)
	{
		throw new UnsupportedOperationException();
	}

	public HGQueryCondition getCondition()
	{
		return cond;
	}
	
	public Mapping<?, ?> getMapping()
	{
		return mapping;
	}
	
	public int hashCode() 
	{ 
		return HGUtils.hashThem(cond, mapping);  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof MapCondition))
			return false;		
		else
		{
			MapCondition c = (MapCondition)x;
			return HGUtils.eq(cond, c.cond) &&
				   HGUtils.eq(mapping, c.mapping);
		}
	}
}
