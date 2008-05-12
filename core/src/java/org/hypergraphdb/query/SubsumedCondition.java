/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.type.HGAtomType;

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
	private HGHandle general;
	private Object generalValue;
	private HGAtomPredicate impl;
	
	private final class AtomBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph hg, HGHandle specific)
		{
			HGHandle specificType = hg.getType(specific);
			
			if (generalValue == null)
			{
				return ((HGAtomType)hg.get(hg.getType(specific))).subsumes(null, hg.get(specific));
			}
			else
			{
				HGHandle h = hg.getHandle(generalValue);
				HGHandle generalType;
				if (h == null)
					 generalType = hg.getTypeSystem().getTypeHandle(generalValue.getClass());
				else
				{
					generalType = hg.getType(h);
					if (declaredSubsumption(hg, h, specific))
						return true;
				}
				if (!generalType.equals(specificType))
					return false;
				else
					return ((HGAtomType)hg.get(hg.getType(specific))).subsumes(generalValue, hg.get(specific));
			}
		}
	}
	
	private final class HandleBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph hg, HGHandle specific)
		{
			if (declaredSubsumption(hg, general, specific))
				return true;
			HGHandle specificType = hg.getType(specific);
			HGHandle generalType = hg.getType(general);
			if (!generalType.equals(specificType))
				return false;
			else
				return ((HGAtomType)hg.get(generalType)).subsumes(hg.get(general), hg.get(specific));
		}
	}
	
	public SubsumedCondition(Object generalValue)
	{
		this.generalValue = generalValue;
		impl = new AtomBased();
	}
	
	public SubsumedCondition(HGHandle general)
	{
		this.general = general;
		impl = new HandleBased();
	}
	
	public HGHandle getGeneralHandle()
	{
		return general;
	}
	
	public Object getGeneralValue()
	{
		return generalValue;
	}
	
	public final boolean satisfies(HyperGraph hg, HGHandle specific) 
	{
		return impl.satisfies(hg, specific);
	}
	
	public int hashCode() 
	{ 
		return general.hashCode();
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof SubsumedCondition))
			return false;
		else
		{
			SubsumedCondition c = (SubsumedCondition)x;
			return general.equals(c.general);
		}
	}
}