/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.query;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.util.Ref;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * <p>
 * A <code>LinkCondition</code> constraints the query result set to links
 * pointing to a target set of atoms. The target set is specified when
 * the condition is constructed through an array of <code>HGHandle</code>s.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class LinkCondition implements HGQueryCondition, HGAtomPredicate 
{
	private Set<Ref<HGHandle>> targetSet = null;
	
	public LinkCondition()
	{
		
	}
	
	public LinkCondition(HGLink link)
	{
		targetSet = new HashSet<Ref<HGHandle>>();
		for (int i = 0; i < link.getArity(); i++)
			targetSet.add(hg.constant(link.getTargetAt(i)));
	}
	
	public LinkCondition(HGHandle...targets)
	{
		if (targets == null)
			throw new HGException("LinkCondition instantiated with a null target set.");
        this.targetSet = new HashSet<Ref<HGHandle>>();
        for (int i = 0; i < targets.length; i++)
			targetSet.add(hg.constant(targets[i]));
	}
	
	public LinkCondition(Ref<HGHandle>...targets)
	{
        this.targetSet = new HashSet<Ref<HGHandle>>();
        for (int i = 0; i < targets.length; i++)
			targetSet.add(targets[i]);
	}
	
	public LinkCondition(Collection<HGHandle> targets)
	{
        if (targets == null)
            throw new HGException("LinkCondition instantiated with a null target set.");
        this.targetSet = new HashSet<Ref<HGHandle>>();
        for (HGHandle h : targets)
        targetSet.add(hg.constant(h));
	}
	
	public boolean contains(HGHandle h)
	{
		for (Ref<HGHandle> r : targetSet)
			if (h.equals(r.get()))
				return true;
		return false;
	}
	
	public Set<Ref<HGHandle>> targets()
	{
		return targetSet;
	}
	
	
	public Set<Ref<HGHandle>> getTargetSet()
	{
		return targetSet;
	}
	
	public void setTargetSet(Set<Ref<HGHandle>> targetSet)
	{
		this.targetSet = targetSet;
	}
	
	/**
	 * <p>Return <code>true</code> if <code>handle</code> points to a link whose
	 * target set is a superset of this condition's <code>targetSet</code>.</p>
	 */
	public boolean satisfies(HyperGraph graph, HGHandle handle) 
	{
		int count = 0;
		
		// Since we have a set of references here, a simple 'contains' check won't work
		// so we need to loop through the target to check for each element of the link
		// under consideration. This is probably fast enough for small sets, but for larger
		// sets it may be performance issue. One option if the build a set of handles
		// if targetSet is large, each time satisfies is called. For this to be justified,
		// targetSet has to be especially large, say > 100. So far, I have never seen
		// a case like this.
		
		// If the atom corresponding to 'handle' is already in the cache, there
		// is no point fetching it from permanent storage. Otherwise, there's no point
		// caching the actual atom...
		if (graph.isLoaded(handle))
		{
			Object atom = graph.get(handle);
			if (! (atom instanceof HGLink))
				return false;
			HGLink link = (HGLink)atom;
			for (int i = 0; i < link.getArity(); i++)
				if (contains(link.getTargetAt(i)))
					count++;
		}
		else
		{
			HGPersistentHandle [] A = graph.getStore().getLink(handle.getPersistent());
			// TODO: this assumes that there are no duplicates in A. Not sure whether
			// it should be forbidden in HyperGraph for a link to contains duplicates.
			// Surely, it doesn't make sense for unordered links - they are just sets. 
			// However, ordered ones might be tricky since they are essentially lists
			// (<=> sets of [position, element] pairs).
			for (int i = 2; i < A.length; i++)
				if (contains(A[i]))
					count++;
		}
		return count == targetSet.size();		
	}
	
	public int hashCode() 
	{ 
		return targetSet.hashCode();  
	}
	
	public boolean equals(Object x)
	{
		if (! (x instanceof LinkCondition))
			return false;
		else
			return ((LinkCondition)x).targetSet.equals(targetSet);
	}
	
	public String toString()
	{
		StringBuffer result = new StringBuffer("links(");
		for (Iterator<Ref<HGHandle>> i = targetSet.iterator(); i.hasNext(); )
		{
			result.append(i.next().get().toString());
			if (i.hasNext())
				result.append(",");
		}
		result.append(")");
		return result.toString();
	}
}