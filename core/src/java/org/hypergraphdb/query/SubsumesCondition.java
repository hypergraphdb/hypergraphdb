/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

/**
 * <p>
 * The <code>SubsumesCondition</code>  examines a given atom and is satisfied
 * if that atom subsumes the atom specified in the condition. 
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class SubsumesCondition extends SubsumesImpl implements HGQueryCondition, HGAtomPredicate 
{
	private Ref<HGHandle> specific;
	private Ref<Object> specificValue;
	private HGAtomPredicate impl;
	
	private final class AtomBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph graph, HGHandle general)
		{
			HGHandle generalType = graph.getType(general);
			
			if (specificValue.get() == null)
			{
				return ((HGAtomType)graph.get(graph.getType(general))).subsumes(graph.get(general), null);
			}
			else
			{
				HGHandle h = graph.getHandle(specificValue.get());
				HGHandle specificType;
				if (h == null)
					 specificType = graph.getTypeSystem().getTypeHandle(specificValue.get().getClass());
				else
				{
					specificType = graph.getType(h);
					if (declaredSubsumption(graph, general, h))
						return true;
				}
				if (!specificType.equals(generalType))
					return false;
				else
					return ((HGAtomType)graph.get(graph.getType(general))).subsumes(graph.get(general), specificValue.get());
			}
		}
	}
	
	private final class HandleBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph graph, HGHandle general)
		{
			if (declaredSubsumption(graph, general, specific.get()))
				return true;
			HGHandle specificType = graph.getType(specific.get());
			HGHandle generalType = graph.getType(general);
			if (!generalType.equals(specificType))
				return false;
			else
				return ((HGAtomType)graph.get(generalType)).subsumes(graph.get(general), graph.get(specific.get()));
		}
	}
	
	public SubsumesCondition()
	{
		
	}
	public SubsumesCondition(Object specificValue)
	{
		setSpecificValue(specificValue);
	}
	
	public SubsumesCondition(HGHandle specific)
	{
		setSpecificHandle(specific);
	}
	
	public Ref<HGHandle> getSpecificHandleReference()
	{
		return specific;
	}
	
	public void setSpecificHandleReference(Ref<HGHandle> specific)
	{
		this.specific = specific;
	}
	
	public HGHandle getSpecificHandle()
	{
		return specific == null ? null : specific.get();
	}
	
	public void setSpecificHandle(HGHandle specific)
	{
		this.specific = hg.constant(specific);
		this.specificValue = null;
		if(specific != null) 
			impl = new HandleBased();
	}
	
	public Object getSpecificValue()
	{
		return specificValue == null ? null : specificValue.get();
	}
	
	public void setSpecificValue(Object specificValue)
	{
		this.specificValue = hg.constant(specificValue);
		this.specific = null;
		if (specificValue != null) 
			impl = new AtomBased();

	}
	public final boolean satisfies(HyperGraph hg, HGHandle general) 
	{
		return impl.satisfies(hg, general);
	}
	
	public int hashCode() 
	{ 
		return (specific != null) ? specific.hashCode() : specificValue.hashCode();
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof SubsumesCondition))
			return false;
		else
		{
			SubsumesCondition c = (SubsumesCondition)x;
			if (specific != null)
				return c.specific == null ? false : HGUtils.eq(specific.get(), c.specific.get());
			else if (specificValue != null)
				return c.specificValue == null ? false : HGUtils.eq(specificValue.get(), c.specificValue.get());
			else
				return c.specific == null && c.specificValue == null;

		}
	}
}