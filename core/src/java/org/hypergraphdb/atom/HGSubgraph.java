package org.hypergraphdb.atom;

import java.util.List;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleHolder;
import org.hypergraphdb.HGIndex;
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
import org.hypergraphdb.util.FilteredSortedSet;
import org.hypergraphdb.util.Mapping;

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
    private static final String REVIDX_NAME = "revsubgraph.index";
    
	@HGIgnore
	protected HyperGraph graph;
	protected HGHandle thisHandle;
	Mapping<HGHandle, Boolean> memberPredicate = new Mapping<HGHandle, Boolean>() 
	{
	    public Boolean eval(HGHandle h)
	    {
	        return isMember(h);
	    }
	};
	
	private HGQueryCondition localizeCondition(HGQueryCondition condition)
	{
		return hg.and(new SubgraphMemberCondition(thisHandle), condition);		
	}	
	
	private HGIndex<HGPersistentHandle, HGPersistentHandle> getIndex()
	{
		return graph.getStore().getIndex(IDX_NAME, 
										  BAtoHandle.getInstance(graph.getHandleFactory()), 
										  BAtoHandle.getInstance(graph.getHandleFactory()), 
										  null, 
										  null,
										  true);
	}

    private HGIndex<HGPersistentHandle, HGPersistentHandle> getReverseIndex()
    {
        return graph.getStore().getIndex(REVIDX_NAME, 
                                          BAtoHandle.getInstance(graph.getHandleFactory()), 
                                          BAtoHandle.getInstance(graph.getHandleFactory()), 
                                          null, 
                                          null,
                                          true);
    }
	
	/**
	 * DO NOT USE: internal method, implementation dependent, may disappear at any time.
	 */
	public static HGIndex<HGPersistentHandle, HGPersistentHandle> getReverseIndex(HyperGraph atGraph)
	{
		return atGraph.getStore().getIndex(REVIDX_NAME, 
				  BAtoHandle.getInstance(atGraph.getHandleFactory()), 
				  BAtoHandle.getInstance(atGraph.getHandleFactory()), 
				  null, 
				  null,
				  true);		
	}

    /**
     * DO NOT USE: internal method, implementation dependent, may disappear at any time.
     */
    public static HGIndex<HGPersistentHandle, HGPersistentHandle> getIndex(HyperGraph atGraph)
    {
        return atGraph.getStore().getIndex(IDX_NAME, 
                  BAtoHandle.getInstance(atGraph.getHandleFactory()), 
                  BAtoHandle.getInstance(atGraph.getHandleFactory()), 
                  null, 
                  null,
                  true);        
    }

    
	private void index(HGHandle h)
	{
	    getIndex().addEntry(thisHandle.getPersistent(), h.getPersistent());
	    getReverseIndex().addEntry(h.getPersistent(), thisHandle.getPersistent());
	}

    private void unindex(HGHandle h)
    {
        getIndex().removeEntry(thisHandle.getPersistent(), h.getPersistent());
        getReverseIndex().removeEntry(h.getPersistent(), thisHandle.getPersistent());
    }
	
	public boolean isMember(HGHandle atom)
	{
	    // it's quicker to lookup the reverse index because we expect fewer subgraphs than
	    // atoms in them
		HGRandomAccessResult<HGPersistentHandle> rs = getReverseIndex().find(atom.getPersistent());
		try
		{
			return rs.goTo(thisHandle.getPersistent(), true) == GotoResult.found;
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
	public HGHandle add(final HGHandle atom)
	{
        return graph.getTransactionManager().ensureTransaction(new Callable<HGHandle>() {
            public HGHandle call()
            {
                index(atom);
                return atom;
            }
        });
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
		add(handle);
	}

	public <T> HGSearchResult<T> find(HGQueryCondition condition)
	{
		return graph.find(localizeCondition(condition));
	}

	@SuppressWarnings("unchecked")
	public <T> T findOne(HGQueryCondition condition)
	{
	    return (T)graph.findOne(localizeCondition(condition));
	}

    @SuppressWarnings("unchecked")
	public <T> T getOne(HGQueryCondition condition)
    {
        return (T)graph.getOne(localizeCondition(condition));
    }
	
	@SuppressWarnings("unchecked")
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

	/**
	 * Return incidence set where each element is a member of this <code>HGSubgraph</code>.
	 * The atom itself whose incidence set is returned doesn't have to be a member of the
	 * subgraph!
	 */
	public IncidenceSet getIncidenceSet(HGHandle handle)
	{
		// maybe should return empty set, instead of null? 
	    // but if the atom is not here, one shouldn't be asking for incidence set
		// so null seems more appropriate
	    return new IncidenceSet(handle, 
	                            new FilteredSortedSet<HGHandle>(graph.getIncidenceSet(handle),
	                                                            memberPredicate));
	}

	public HGHandle getType(HGHandle handle)
	{
		return isMember(handle) ? graph.getType(handle) : null;
	}

	/**
	 * Removes the atom globally from the database as well as from the nested graph.
	 * @param handle The atom to remove.
	 * @return The result of {@link HyperGraph.remove}. 
	 */
	public boolean removeGlobally(HGHandle handle)
	{
	    unindex(handle);
	    return graph.remove(handle);
	}

    /**
     * Removes the atom globally from the database as well as from the nested graph.
     * @param handle The atom to remove.
     * @param keepIncidentLinks - whether to also remove links pointing to the removed
     * atom. This parameter applies recursively to the links removed.
     * @return The result of {@link HyperGraph.remove}. 
     */
    public boolean removeGlobally(HGHandle handle, boolean keepIncidentLinks)
    {
        unindex(handle);
        return graph.remove(handle, keepIncidentLinks);
    }
	
	/**
	 * Removes an atom from this scope. The atom is not deleted from the
	 * global {@link HyperGraph} database. If you wish to delete it globally,
	 * use {@link HyperGraph.remove}.
	 * 
	 * @return Return value is unreliable
	 */
	public boolean remove(final HGHandle handle)
	{
	    return graph.getTransactionManager().ensureTransaction(new Callable<Boolean>() 
	    {
	       public Boolean call()
	       {
	           boolean ret = isMember(handle);
	           unindex(handle);
	           return ret;
	       }	       
	    });
	}

	/**
	 * Performs the replace in the global database as this only deals with
	 * an atom's value.
	 */
	public boolean replace(final HGHandle handle, final Object newValue, final HGHandle newType)
	{
        return graph.getTransactionManager().ensureTransaction(new Callable<Boolean>() 
        {
            public Boolean call()
            {	    
                return graph.replace(handle, newValue, newType);
            }
        });
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