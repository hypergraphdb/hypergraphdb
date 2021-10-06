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
 * The <code>SubsumedCondition</code>  examines a given atom and is satisfied
 * if that atom is subsumed by the atom specified in the condition. You can also
 * provide a value instead of a HyperGraph handle by using the <code>SubsumedCondition(Object)</code>
 * constructor.  
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class SubsumedCondition extends SubsumesImpl implements HGQueryCondition, HGAtomPredicate 
{
	private Ref<HGHandle> general;
	private Ref<Object> generalValue;
	private HGAtomPredicate impl;
	
	private final class AtomBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph hg, HGHandle specific)
		{
			HGHandle specificType = hg.getType(specific);
			
			if (generalValue.get() == null)
			{
				return ((HGAtomType)hg.get(hg.getType(specific))).subsumes(null, hg.get(specific));
			}
			else
			{
				HGHandle h = hg.getHandle(generalValue.get());
				HGHandle generalType;
				if (h == null)
					 generalType = hg.getTypeSystem().getTypeHandle(generalValue.get().getClass());
				else
				{
					generalType = hg.getType(h);
					if (declaredSubsumption(hg, h, specific))
						return true;
				}
				if (!generalType.equals(specificType))
					return false;
				else
					return ((HGAtomType)hg.get(hg.getType(specific))).subsumes(generalValue.get(), hg.get(specific));
			}
		}
	}
	
	private final class HandleBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph hg, HGHandle specific)
		{
			if (declaredSubsumption(hg, general.get(), specific))
				return true;
			HGHandle specificType = hg.getType(specific);
			HGHandle generalType = hg.getType(general.get());
			if (!generalType.equals(specificType))
				return false;
			else
				return ((HGAtomType)hg.get(generalType)).subsumes(hg.get(general.get()), hg.get(specific));
		}
	}
	
	public SubsumedCondition()
	{
		
	}
	
	public SubsumedCondition(Object generalValue)
	{
		setGeneralValue(generalValue);
	}

	public SubsumedCondition(Ref<HGHandle> general)
	{
		setGeneralHandleReference(general);
	}
	
	public SubsumedCondition(HGHandle general)
	{
		setGeneralHandle(general);
	}

	public Ref<HGHandle> getGeneralHandleReference()
	{
		return general;
	}
	
	public void setGeneralHandleReference(Ref<HGHandle> general)
	{
		this.general = general;
		this.generalValue = null;
		if (general != null)
			impl = new HandleBased();		
	}
	
	public HGHandle getGeneralHandle()
	{
		return general.get();
	}
	
	public void setGeneralHandle(HGHandle general)
	{
		this.general = hg.constant(general);
		this.generalValue = null;
		if (general != null) 
			impl = new HandleBased();	
	}
	
	public Object getGeneralValue()
	{
		return generalValue == null ? null : generalValue.get();
	}
	
	public void setGeneralValue(Object generalValue)
	{
		this.generalValue = hg.constant(generalValue);
		this.general = null;
		if (generalValue != null) 
			impl = new AtomBased();
	}
	
	public final boolean satisfies(HyperGraph hg, HGHandle specific) 
	{
		return impl.satisfies(hg, specific);
	}
	
	public int hashCode() 
	{ 
		int hash = 0;
		if (general != null)
			hash = HGUtils.hashIt(general.get());
		else if (generalValue != null)
			hash = HGUtils.hashIt(generalValue.get());
		return hash;
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof SubsumedCondition))
			return false;
		else
		{
			SubsumedCondition c = (SubsumedCondition)x;
			if (general != null)
				return c.general == null ? false : HGUtils.eq(general.get(), c.general.get());
			else if (generalValue != null)
				return c.generalValue == null ? false : HGUtils.eq(generalValue.get(), c.generalValue.get());
			else
				return c.general == null && c.generalValue == null;
		}
	}
}