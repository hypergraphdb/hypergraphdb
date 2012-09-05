package hgtest.verify;

import hgtest.TestException;

import java.util.ArrayList;
import java.util.List;

import org.hypergraphdb.*;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.util.HGUtils;

public class LinkConsistency
{
	private HyperGraph graph;
	private HGQueryCondition cond = HGQuery.hg.all();
	private List<HGPersistentHandle> failed = new ArrayList<HGPersistentHandle>();
	private int stopAfter = 1;
	private boolean ignoreCache = false;
	private boolean forceCache = false;
	private boolean ignoreMissing = false;
	
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
		
	public boolean isForceCache()
	{
		return forceCache;
	}

	public void setForceCache(boolean forceCache)
	{
		this.forceCache = forceCache;
	}

	public HyperGraph getGraph()
	{
		return graph;
	}

	public void setGraph(HyperGraph graph)
	{
		this.graph = graph;
	}

	public boolean isIgnoreMissing()
	{
		return ignoreMissing;
	}

	public void setIgnoreMissing(boolean ignoreMissing)
	{
		this.ignoreMissing = ignoreMissing;
	}

	public List<HGPersistentHandle> getFailed()
	{
		return failed;
	}

	/**
	 * <p>Return true if 'link' is a member of the incidence set of 'target' and
	 * false otherwise.</p> 
	 */
	public boolean isIncidenceSetMember(HGPersistentHandle link, HGPersistentHandle target)
	{
		IncidenceSet is = null;
		if (forceCache || (!ignoreCache && graph.isIncidenceSetLoaded(target)))
		{
			is = graph.getIncidenceSet(target);
			HGSearchResult<HGHandle> rs = is.getSearchResult();
			try
			{
				while (rs.hasNext())
					if (rs.next().equals(link))
						return true;
			}
			finally
			{
				rs.close();
			}
		}
		else
		{
			HGSearchResult<HGPersistentHandle> rs = graph.getStore().getIncidenceResultSet(target);
			try
			{
			    while (rs.hasNext())
			        if (rs.next().equals(link))
			            return true;
			}
			finally
			{
			    rs.close();
			}
		}
		return false;
	}
	
	public boolean isLinkMember(HGPersistentHandle link, HGPersistentHandle target)
	{		
		if (ignoreCache || !graph.isLoaded(link))
		{
			HGPersistentHandle [] layout = graph.getStore().getLink(link);
			if (layout == null && !ignoreMissing)
				throw new TestException("The atom handle " + link + 
						" is refered from incidence set of " + target + 
						" is not available from the HGStore.");
			for (int i = 2; i < layout.length; i++)
				if (layout[i].equals(target))
					return true;
			return false;
		}
		Object x = graph.get(link);
		if (! (x instanceof HGLink))
			throw new TestException("Atom " + link + 
					" is not a link, but it is a member of the incidence set of " + target);
		HGLink l = (HGLink)x;
		for (int i = 0; i < l.getArity(); i++)
			if (l.getTargetAt(i).equals(target))
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
			if (layout == null && !ignoreMissing)
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
	
	public boolean isIncidenceSetConsistent(HGPersistentHandle atom)
	{
		HGAtomSet is = null;
		if (!forceCache && (ignoreCache || !graph.isIncidenceSetLoaded(atom)))
		{
		    is = new HGAtomSet();
		    HGSearchResult<HGPersistentHandle> rs = graph.getStore().getIncidenceResultSet(atom);
		    try { while (rs.hasNext()) is.add(rs.next()); }
		    finally { rs.close(); }
		}
		else
			is = graph.getIncidenceSet(atom);
		if (is == null)
			return true;
		HGSearchResult<HGHandle> rs = is.getSearchResult();
		try
		{
			while (rs.hasNext())
				if (!isLinkMember(graph.getPersistentHandle(rs.next()), atom))
					return false;
			return true;
		}
		finally
		{
			rs.close();
		}
	}
	
	public void checkConsistency()
	{
		failed.clear();
		int total = 0;
		HGSearchResult<HGPersistentHandle> rs = graph.find(cond);
		try
		{
			while (rs.hasNext() && failed.size() < stopAfter)
			{
				total++;
				rs.next();
				if (!isLinkConsistent(rs.current()) || !isIncidenceSetConsistent(rs.current()))
					failed.add(rs.current());
			}
			System.out.println("Total tested: " + total);
		}
		finally
		{
			HGUtils.closeNoException(rs);
		}
	}
	
}