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
import org.hypergraphdb.HGPersistentHandle;

/**
 * <p>
 * The <code>IncidentCondition</code> specifies that a search result atom 
 * should be a member of the incidence set of a given atom. By itself the 
 * condition would yield a query that enumerates all links pointing to
 * the specified atom.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class IncidentCondition implements HGQueryCondition, HGAtomPredicate 
{
	private HGHandle target;
	
	public IncidentCondition(HGHandle target)
	{
		this.target = target;
	}
	
	public HGHandle getTarget()
	{
		return target;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle handle) 
	{
		//
		// This satisfies if 'handle' is a link pointing to target.
		// So we simply fetch the target set of 'handle' and check whether
		// 'target' is part of it.
		//
		HGPersistentHandle targetPHandle = hg.getPersistentHandle(target);
		HGPersistentHandle [] targetSet = hg.getStore().getLink(hg.getPersistentHandle(handle));
		for (int i = 2; i < targetSet.length; i++)
			if (targetPHandle.equals(targetSet[i]))
				return true;
		return false;
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("linksTo(");
		result.append(target);
		result.append(")");
		return result.toString();
	}
}