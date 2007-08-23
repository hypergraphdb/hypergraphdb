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
 * The <code>SubsumesCondition</code>  examines a given atom and is satisfied
 * if that atom subsumes the atom specified in the condition. 
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class SubsumesCondition extends SubsumesImpl implements HGQueryCondition, HGAtomPredicate 
{
	private HGHandle specific;
	private Object specificValue;
	private HGAtomPredicate impl;
	
	private final class AtomBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph hg, HGHandle general)
		{
			HGHandle generalType = hg.getType(general);
			
			if (specificValue == null)
			{
				return ((HGAtomType)hg.get(hg.getType(general))).subsumes(hg.get(general), null);
			}
			else
			{
				HGHandle h = hg.getHandle(specificValue);
				HGHandle specificType;
				if (h == null)
					 specificType = hg.getTypeSystem().getTypeHandle(specificValue.getClass());
				else
				{
					specificType = hg.getType(h);
					if (declaredSubsumption(hg, general, h))
						return true;
				}
				if (!specificType.equals(generalType))
					return false;
				else
					return ((HGAtomType)hg.get(hg.getType(general))).subsumes(hg.get(general), specificValue);
			}
		}
	}
	
	private final class HandleBased implements HGAtomPredicate
	{
		public boolean satisfies(HyperGraph hg, HGHandle general)
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
	
	public SubsumesCondition(Object specificValue)
	{
		this.specificValue = specificValue;
		impl = new AtomBased();
	}
	
	public SubsumesCondition(HGHandle specific)
	{
		this.specific = specific;
		impl = new HandleBased();
	}
	
	public HGHandle getSpecificHandle()
	{
		return specific;
	}
	
	public Object getSpecificValue()
	{
		return specificValue;
	}
	
	public final boolean satisfies(HyperGraph hg, HGHandle general) 
	{
		return impl.satisfies(hg, general);
	}
}