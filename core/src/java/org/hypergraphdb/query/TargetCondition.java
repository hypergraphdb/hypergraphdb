/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Ref;

/**
 * 
 * <p>
 * A query condition that constraints the result set to atoms that are targets to
 * a specific link. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class TargetCondition implements HGQueryCondition, HGAtomPredicate
{
	private Ref<HGHandle> link;

	public TargetCondition()
	{
		
	}
	
	public TargetCondition(Ref<HGHandle> link)
	{
		this.link = link;
	}
	
	public TargetCondition(HGHandle link)
	{
		this.link = hg.constant(link);
	}

	public Ref<HGHandle> getLinkReference()
	{
		return link;
	}
	
	public void setLinkReference(Ref<HGHandle> link)
	{
		this.link = link;
	}
	
	public HGHandle getLink()
	{
		return link.get();
	}
	
	public void setLink(HGHandle link)
	{
		this.link = hg.constant(link);
	}

	public boolean satisfies(HyperGraph graph, HGHandle handle)
	{
		if (graph.isLoaded(link.get()))
		{
			HGLink l = (HGLink)graph.get(link.get());
			for (int i = 0; i < l.getArity(); i++)
				if (l.getTargetAt(i).equals(handle))
					return true;
		}
		else
		{
			HGPersistentHandle [] l = graph.getStore().getLink(graph.getPersistentHandle(link.get()));
			if (l != null) for (HGHandle h : l)
				if (h.equals(handle))
					return true;
		}
		return false;
	}
	
	public int hashCode() 
	{ 
		return link.hashCode();
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof TargetCondition))
			return false;
		else
		{
			TargetCondition c = (TargetCondition)x;
			return HGUtils.eq(link.get(), c.link.get());
		}
	}	
}