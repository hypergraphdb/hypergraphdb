package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.HGUtils;

public class DelayedSetLoadPredicate implements HGAtomPredicate
{
	private HGQuery query;
	private HGAtomSet set = null;
	
	private void loadSet()
	{
		HGSearchResult rs = null;
		try
		{
			set = new HGAtomSet();
			rs = query.execute();
			while (rs.hasNext())
				set.add((HGHandle)rs.next());
		}
		finally
		{
			HGUtils.closeNoException(rs);
		}
	}
	
	public DelayedSetLoadPredicate(HGQuery query)
	{
		this.query = query;
	}
	
	public boolean satisfies(HyperGraph hg, HGHandle handle)
	{
		if (set == null)
			loadSet();
		return set.contains(handle);
	}
}