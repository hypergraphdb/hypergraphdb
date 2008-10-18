package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.query.HGAtomPredicate;
import org.hypergraphdb.util.HGUtils;

/**
 * 
 * <p>
 * A predicate that check whether a handle is the member of a set
 * of handles. The set of handles itself is lazily evaluated by running
 * a <code>HGQuery</code> the first time the <code>satisfies</code> is called.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class DelayedSetLoadPredicate implements HGAtomPredicate
{
	private HGQuery<HGHandle> query;
	private HGAtomSet set = null;
	
	private void loadSet()
	{
		HGSearchResult<HGHandle> rs = null;
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
	
	public DelayedSetLoadPredicate(HGQuery<HGHandle> query)
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