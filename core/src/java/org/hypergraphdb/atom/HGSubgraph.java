package org.hypergraphdb.atom;

import java.util.List;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleHolder;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HyperNode;
import org.hypergraphdb.IncidenceSet;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.hypergraphdb.annotation.HGIgnore;
import org.hypergraphdb.query.HGQueryCondition;
import org.hypergraphdb.query.SubgraphMemberCondition;
import org.hypergraphdb.storage.BAtoHandle;

/**
 * 
 * <p>
 * A {@link HyperNode} that encapsulates a set of atoms from the global
 * {@link HyperGraph} database. A subgraph can be thought of as a hyper-edge
 * in the standard set-theoretic formulation of hypergraphs.  
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class HGSubgraph implements HyperNode, HGHandleHolder, HGGraphHolder
{
	private static final String IDX_NAME = "subgraph.index";
	
	@HGIgnore
	HyperGraph graph;
	HGHandle thisHandle;

	private HGQueryCondition localizeCondition(HGQueryCondition condition)
	{
		return hg.and(new SubgraphMemberCondition(thisHandle), condition);		
	}	
	
	private HGBidirectionalIndex<HGPersistentHandle, HGPersistentHandle> getIndex()
	{
		return graph.getStore().getBidirectionalIndex(IDX_NAME, 
													  BAtoHandle.getInstance(graph.getHandleFactory()), 
													  BAtoHandle.getInstance(graph.getHandleFactory()), 
													  null, 
													  true);
	}
	
	/**
	 * DO NOT USE: internal method, implementation dependent, may disappear at any time.
	 */
	public static HGBidirectionalIndex<HGPersistentHandle, HGPersistentHandle> getIndex(HyperGraph atGraph)
	{
		return atGraph.getStore().getBidirectionalIndex(IDX_NAME, 
				  BAtoHandle.getInstance(atGraph.getHandleFactory()), 
				  BAtoHandle.getInstance(atGraph.getHandleFactory()), 
				  null, 
				  true);		
	}
	
	public boolean isMember(HGHandle atom)
	{
		HGRandomAccessResult<HGPersistentHandle> rs = getIndex().find(thisHandle.getPersistent());
		try
		{
			return rs.goTo(atom.getPersistent(), true) == GotoResult.found;
		}
		finally
		{
			rs.close();
		}		
	}
	
	/**
	 * <p>
	 * Add an existing atom to this {@link HyperNode}. The atom may be a member
	 * multiple nodes at a time.
	 * </p>
	 * 
	 * @param atom
	 * @return The <code>atom</code> parameter.
	 */
	public HGHandle add(HGHandle atom)
	{
		getIndex().addEntry(thisHandle.getPersistent(), atom.getPersistent());
		return atom;
	}

	/**
	 * Add to global graph and mark as member of this subgraph.
	 */
	public HGHandle add(Object atom, HGHandle type, int flags)
	{
		return add(graph.add(atom, type, flags));
	}

	public long count(HGQueryCondition condition)
	{
		return hg.count(graph, localizeCondition(condition));
	}

	/**
	 * Define in global graph and mark as member of this subgraph.
	 */
	public void define(HGHandle handle, 
					   HGHandle type, 
					   Object instance,
					   int flags)
	{
		graph.define(handle, type, instance, flags);
	}

	public <T> HGSearchResult<T> find(HGQueryCondition condition)
	{
		return graph.find(localizeCondition(condition));
	}

	public List<HGHandle> findAll(HGQueryCondition condition)
	{
		return graph.findAll(localizeCondition(condition));
	}

	public <T> List<T> getAll(HGQueryCondition condition)
	{
		return graph.getAll(localizeCondition(condition));
	}
	
	@SuppressWarnings("unchecked")
	public <T> T get(HGHandle handle)
	{
		return isMember(handle) ? (T)graph.get(handle) : null;
	}

	public IncidenceSet getIncidenceSet(HGHandle handle)
	{
		// maybe should return empty set, instead of null? 
	    // but if the atom is not here, one shouldn't be asking for incidence set
		// so null seems more appropriate
		return isMember(handle) ? graph.getIncidenceSet(handle) : null;
	}

	public HGHandle getType(HGHandle handle)
	{
		return isMember(handle) ? graph.getType(handle) : null;
	}

	/**
	 * Removes an atom from this scope. The atom is not deleted from the
	 * global {@link HyperGraph} database. If you wish to delete it globally,
	 * use {@link HyperGraph.remove}.
	 * 
	 * @return Return value is unreliable
	 */
	public boolean remove(HGHandle handle)
	{
		boolean ret = isMember(handle);
		getIndex().removeEntry(thisHandle.getPersistent(), handle.getPersistent());
		return ret;
	}

	/**
	 * Performs the replace in the global database as this only deals with
	 * an atom's value.
	 */
	public boolean replace(HGHandle handle, Object newValue, HGHandle newType)
	{
		return graph.replace(handle, newValue, newType);
	}

	public HGHandle getAtomHandle()
	{
		return this.thisHandle;
	}

	public void setAtomHandle(HGHandle handle)
	{
		this.thisHandle = handle;
	}

	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	}
	
	public HyperGraph getHyperGraph()
	{
		return this.graph;
	}
}