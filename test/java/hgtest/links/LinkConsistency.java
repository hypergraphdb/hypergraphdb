package hgtest.links;

import hgtest.TestException;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.*;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.HGUtils;

public class LinkConsistency
{
	private HyperGraph graph;
	private HGQueryCondition cond = HGQuery.hg.all();
	private List<HGPersistentHandle> failed = new ArrayList<HGPersistentHandle>();
	private int stopAfter = 1;
	private boolean ignoreCache = false;
	
	public LinkConsistency(HyperGraph graph)
	{
		this.graph = graph;		
	}
	
	public int getStopAfter()
	{
		return stopAfter;
	}

	public void setStopAfter(int stopAfter)
	{
		this.stopAfter = stopAfter;
	}

	public HGQueryCondition getCond()
	{
		return cond;
	}

	public void setCond(HGQueryCondition cond)
	{
		this.cond = cond;
	}


	public boolean isIgnoreCache()
	{
		return ignoreCache;
	}

	public void setIgnoreCache(boolean ignoreCache)
	{
		this.ignoreCache = ignoreCache;
	}

	/**
	 * <p>Return true if 'link' is a member of the incidence set of 'target' and
	 * false otherwise.</p> 
	 */
	public boolean isIncidenceSetMember(HGPersistentHandle link, HGPersistentHandle target)
	{
		HGHandle [] is = null;
		if (!ignoreCache && graph.isIncidenceSetLoaded(target))
			is = graph.getIncidenceSet(target);
		else
			is = graph.getStore().getIncidenceSet(target);
		for (HGHandle h : is)
			if (h.equals(link))
				return true;
		return false;
	}
	
	/**
	 * <p>Check if the link is a member of all its targets' incidence sets. An atom
	 * that is not a link has (trivially) consistent linkage.</p> 
	 */
	public boolean isLinkConsistent(HGPersistentHandle link)
	{
		if (ignoreCache || !graph.isLoaded(link))
		{
			HGPersistentHandle [] layout = graph.getStore().getLink(link);
			if (layout == null)
				throw new TestException("The atom handle " + link + " is not available from the HGStore.");
			for (int i = 2; i < layout.length; i++)
				if (!isIncidenceSetMember(link, layout[i]))
					return false;
		}
		else
		{
			Object x = graph.get(link);
			if (!(x instanceof HGLink))
				return true;
			HGLink l = (HGLink)x;
			for (int i = 0; i < l.getArity(); i++)
				if (!isIncidenceSetMember(link, graph.getPersistentHandle(l.getTargetAt(i))))
					return false;
		}
		return true;		
	}
	
	public void checkConsistency()
	{
		failed.clear();
		HGSearchResult<HGPersistentHandle> rs = graph.find(cond);
		try
		{
			while (rs.hasNext() && failed.size() < stopAfter)
			{
				if (!isLinkConsistent(rs.next()))
					failed.add(rs.current());
			}
		}
		finally
		{
			HGUtils.closeNoException(rs);
		}
	}
}