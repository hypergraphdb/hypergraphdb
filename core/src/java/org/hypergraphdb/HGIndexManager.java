/* 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.*;
import org.hypergraphdb.maintenance.ApplyNewIndexer;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;

/**
 * <p>
 * The <code>HGIndexManager</code> allows you to create atom indices. Such indices
 * allow quick lookup of atoms depending on their value and/or target sets.  
 * </p>
 * <p>
 * Atom indexing in {@link HyperGraph} relies on the concept of a {@link HGIndexer}. Indexers
 * are always bound to an atom type and they are responsible for producing an index <em>key</em> 
 * given an atom instance. The index manager interacts with the {@link HGStore} to create
 * and update indices at the storage level based on an implementation of the {@link HGIndexer}
 * interface.    
 * </p>
 * <p>
 * When an implementation of a {@link HGIndexer} does not produce keys of <code>byte[]</code>
 * type, it is required to return a non-null {@link ByteArrayConverter} from its
 * <code>getConverter</code> method. In addition, if a regular byte ordering is not appropriate
 * for a given key type, the {@link HGIndexer} implementation must return a non-null
 * <code>Comparator</code> from its <code>getComparator</code> method. 
 * </p>
 * <p>
 * To create a new index for a given atom type, call the <code>register(HGIndexer)</code> method.
 * To later remove it, call the <code>unregister(HGIndexer)</code> method. Registering an
 * indexer will store the indexer as a HyperGraph atom and it will request a low-level storage
 * index from the {@link HGStore}. Atom instances of the type for which a new indexer
 * is being registered will be automatically indexed henceforth. If there are already atoms of that
 * type in the database, they be indexed the next time the database is opened. You can force this
 * indexing of existing data to happen right away by calling the <code>runMaintenance</code> of 
 * the {@link HyperGraph} instance.
 * </p>
 * <p>
 * <strong>NOTE</strong>: this class is not thread safe and its methods do not participate in database
 * transactions. Modification of the database indexing schema are meant to be used during initialization
 * or other times when there's no other database activity. 
 * </p>
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class HGIndexManager 
{
	private HyperGraph graph;	
	private HashMap<HGIndexer, HGIndex<? extends Object, ? extends Object>> indices = 
			new HashMap<HGIndexer, HGIndex<? extends Object, ? extends Object>>();	
	private HashMap<HGHandle, List<HGIndexer<?,?>>> indexers = new HashMap<HGHandle, List<HGIndexer<?,?>>>();
	
	private String getIndexName(HGIndexer indexer)
	{
		return graph.getPersistentHandle(indexer.getType()) + "_" + 
			   graph.getPersistentHandle(graph.getHandle(indexer));
	}
	
	private HGIndexer toAtomIndexer(HGIndexer indexer)
	{
		List<HGIndexer<?,?>> L = indexers.get(indexer.getType());
		int i = L.indexOf(indexer);
		return i >= 0 ? L.get(i) : null;
	}
	
	private <KeyType extends Object, ValueType extends Object> 
		HGIndex<KeyType, ValueType> getOrCreateIndex(HGIndexer<?,?> indexer)
	{
		HGIndex<KeyType, ValueType> result = (HGIndex<KeyType, ValueType>)indices.get(indexer);
		if (result == null)			
		{
			String name = getIndexName(indexer);
			ByteArrayConverter<ValueType> converter = null;
			if (indexer instanceof HGValueIndexer)
				converter = (ByteArrayConverter<ValueType>)((HGValueIndexer)indexer).getValueConverter(graph);
			else
				converter = (ByteArrayConverter<ValueType>)BAtoHandle.getInstance(graph.getHandleFactory());
			result = graph.getStore().getIndex(name, 
											   (ByteArrayConverter<KeyType>)indexer.getConverter(graph), 
											   converter, 
											   indexer.getComparator(graph),
											   null, 
											   true);
//			if (result == null)
//				result = graph.getStore().createIndex(name, 
//													  (ByteArrayConverter<KeyType>)indexer.getConverter(graph), 
//													  converter, 
//													  indexer.getComparator(graph));
			indices.put(indexer, result);
		}
		return result;
	}
		
	private void removeFromSubtypes(HGIndexer indexer)
	{
	    for (HGHandle currentType : hg.typePlus(indexer.getType()).getSubTypes(graph))
        {
	        if (currentType.equals(indexer.getType()))
	            continue;
            List<HGIndexer<?,?>> forType = indexers.get(currentType);
            if (forType != null)
            {
                forType.remove(indexer);
                if (forType.isEmpty())
                    indexers.remove(currentType);
            }
        }
	}
	
	private void deleteIndex(HGIndexer indexer)
	{
		indexer = toAtomIndexer(indexer);
		if (indexer == null)
			return;
		indices.remove(indexer);
		String name = getIndexName(indexer);
		graph.getStore().removeIndex(name);
	}
	
	public HGIndexManager(HyperGraph graph)
	{
		this.graph = graph;
	}
	
	void loadIndexers()
	{
		HGHandle indexerBaseType = graph.getTypeSystem().getTypeHandle(HGIndexer.class);
		// First get call HGIndexer types.
		List<HGHandle> indexerTypes = hg.findAll(graph, hg.subsumed(indexerBaseType));
		for (HGHandle indexerType : indexerTypes)
		{
			List<HGHandle> indexerAtoms = hg.findAll(graph, hg.type(indexerType));
			for (HGHandle hindexer : indexerAtoms)
			{
				HGIndexer indexer = graph.get(hindexer);
			    // While an indexer is defined for a specific type T, we have
			    // to also index all atoms with a subtype of T. 
			    for (HGHandle currentType : hg.typePlus(indexer.getType()).getSubTypes(graph))
			    {
			    
			    // the lookup of the indexer by subtype doesn't work because the HGIndexer.equals
			    // method compare the type handles, so there's no point in associated the indexer
			    // with the sub-types here.
    				List<HGIndexer<?,?>> forType = indexers.get(currentType);
    				if (forType == null)
    				{
    					forType = new ArrayList<HGIndexer<?,?>>();
    					indexers.put(currentType, forType);
    				}
    				forType.add(indexer);
			    }
			}
		}
	}
	
	/**
	 * <p>
	 * Cleanup reference to external resources for this indexer.
	 * </p>
	 */
	public void close()
	{
		// NOP
	}
	
	/**
	 * <p>
	 * Remove an existing index. If there's no index as specified by
	 * the <code>indexer</code> parameter, nothing is done.
	 * </p>
	 * 
	 * @param indexer The indexer to be removed.
	 * @return <code>true</code> if the indexer was removed and
	 * <code>false</code> if it didn't exist.
	 */
	public boolean unregister(HGIndexer indexer)
	{
	    removeFromSubtypes(indexer);
		List<HGIndexer<?,?>> forType = indexers.get(indexer.getType());
		if (forType == null)
			return false;
		int i = forType.indexOf(indexer);
		if (i < 0)
			return false;
		HGHandle hIndexer = graph.getHandle(forType.get(i));
		
		// Make sure there's no MaintenanceOperation based on this indexer 
		// currently scheduled.
		HGHandle maintenanceOp = hg.findOne(graph, 
										    hg.and(hg.type(ApplyNewIndexer.class), 
										    	   hg.eq("hindexer", hIndexer)));
		if (maintenanceOp != null)
			graph.remove(maintenanceOp);
		deleteIndex(forType.get(i));	
		graph.remove(hIndexer);		
		forType.remove(i);		
        if (forType.isEmpty())
            indexers.remove(indexer.getType());     		
		return true;
	}
	
	/**
	 * <p>
	 * Remove all indexers for the given type. This is normally called
	 * only when the type is being from the HyperGraph instance.
	 * </p>
	 * 
	 * @param typeHandle The handle of the atom type whose indexers are to be
	 * deleted.
	 */
	public void unregisterAll(HGHandle typeHandle)
	{
		List<HGIndexer<?,?>> forType = indexers.get(typeHandle);
		if (forType != null)
		{
			for (Iterator<HGIndexer<?,?>> i = forType.iterator(); i.hasNext(); )
			{
				HGIndexer<?,?> indexer = i.next();
		        removeFromSubtypes(indexer);
				deleteIndex(indexer);
				graph.remove(graph.getHandle(indexer));				
				i.remove();
			}
			if (forType.isEmpty())
			    indexers.remove(typeHandle);
		}
	}
	
	/**
	 * <p>Return <code>true</code> if the given <code>HGIndexer</code> is registered
	 * with the index manager and <code>false</code> otherwise.  
	 * @param indexer The possibly registered <code>HGIndexer</code>. 
	 */
	public boolean isRegistered(HGIndexer<?,?> indexer)
	{
		List<HGIndexer<?,?>> forType = indexers.get(indexer.getType());
		return forType == null ? false : forType.contains(indexer);
	}
	
	/**
	 * <p>
	 * Possibly create a new index based on the specified <code>IndexDescriptor</code>.
	 * If an index corresponding to the descriptor already exists, the method
	 * does nothing and returns <code>false</code>. Otherwise it creates a new index which
	 * will become active right away if there's no data with the specified <code>HGIndexer</code>'s
	 * type. If there is already some data with the type (or sub-types) being indexed, the index
	 * will become active the next time the database is opened when a potentially long indexing
	 * operation will be triggered. If you want to do the indexing right after creating an index
	 * on existing data, call the <code>HyperGraph.runMaintenance</code> method.
	 * </p>
	 * 
	 * @param desc The descriptor of the index to be created.
	 * @return <code>true</code> if a new index was created and <code>false</code>
	 * otherwise.
	 */
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> register(HGIndexer<?,?> indexer)
	{
	    boolean createNewIndex = false;
	    boolean activate  = hg.count(graph, hg.typePlus(indexer.getType())) == 0;
	    
        for (HGHandle currentType : hg.typePlus(indexer.getType()).getSubTypes(graph))
        {
            List<HGIndexer<?,?>> forType = indexers.get(currentType);
            if (forType == null)
            {
                forType = new ArrayList<HGIndexer<?,?>>();
                indexers.put(currentType, forType);
            }
            if (!forType.contains(indexer))
            {
                if (currentType.equals(indexer.getType()))
                    createNewIndex = true;
                if (activate)
                	forType.add(indexer);
            }
        }
	    
		if (createNewIndex)
		{
			HGHandle hIndexer = graph.add(indexer);			
			HGIndex<KeyType, ValueType> idx = getOrCreateIndex(indexer);
			if (!activate)
				graph.add(new ApplyNewIndexer(hIndexer));
			return idx;
		}
		else
		{
			return getIndex(indexer);
		}
	}

	/**
	 * <p>
	 * Register a newly created sub-type for indexing along with a super-type. All indexers
	 * of the parent type will also apply to the sub-type. This binding of indexers to 
	 * the sub-types of a type for which they were originally defined is normally done at
	 * startup time. However, when a sub-type is created, of a type that is already somehow
	 * indexed, one needs to explicitly bind the indexers at least until the next startup time.  
	 * </p>
	 * 
	 * @param superType The handle of the base type.
	 * @param subType The handle of the sub-type.
	 */
	void registerSubtype(HGHandle superType, HGHandle subType)
	{
	    List<HGIndexer<?,?>> forSuperType = indexers.get(superType);
	    if (forSuperType == null)
	        return;
	    else if (forSuperType.isEmpty())
	        indexers.remove(forSuperType);
	    List<HGIndexer<?,?>> forSubType = indexers.get(subType);
	    if (forSubType == null)
	    {
	        forSubType = new ArrayList<HGIndexer<?,?>>();
            indexers.put(subType, forSubType);	        
	    }
	    for (HGIndexer<?,?> idx : forSuperType)
	        if (!forSubType.contains(idx))
	            forSubType.add(idx);
	}
	
	/**
	 * <p>
	 * Retrieve the storage <code>HGIndex</code> associated to the passed-in
	 * <code>HGIndexer</code>.
	 * </p>
	 * @param indexer The <code>HGIndexer</code> whose associated <code>HGIndex</code>
	 * is desired.
	 * @return The method will return <code>null</code> if the passed in <code>HGIndexer</code>
	 * hasn't been registered with the index manager. 
	 */
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(HGIndexer<?,?> indexer)
	{
		HGIndex<KeyType, ValueType> result = (HGIndex<KeyType, ValueType>)indices.get(indexer);
		if (result == null)
		{
			List<HGIndexer<?,?>> L = indexers.get(indexer.getType());			
			if (L != null)
			{
				int i = L.indexOf(indexer);
				if (i >= 0)				
					result = getOrCreateIndex(L.get(i));
			}				
		}
		return result;
	}

	/**
	 * <p>
	 * Return all registered <code>HGIndexer</code>s for a given HyperGraph type.
	 * </p>
	 * 
	 * @param type The <code>HGHandle</code> of the HyperGraph type whose indexers
	 * are desired.
	 * @return The list of indexers. May be <code>null</code> if no indexer is
	 * currently registered for that type.
	 */
	public List<HGIndexer<?,?>> getIndexersForType(HGHandle type)
	{
		return this.indexers.get(type);
	}

	/**
	 * <p>Return the predefined index from types to atoms. That is, each key
	 * in this index is a type handle and its values are all atoms with that type.</p>
	 */
	public HGIndex<HGPersistentHandle, HGPersistentHandle> getIndexByType()
	{
		return graph.indexByType;
	}

	/** 
	 * <p>Return the predefined index from values to atoms. That is, each key
	 * in this index is a value handle and its <em> index values</em> are all atoms 
	 * with that value handle.</p> 
	 */
	public HGIndex<HGPersistentHandle, HGPersistentHandle> getIndexByValue()
	{
		return graph.indexByValue;
	}
	
	/**
	 * <p>
	 * Called when an atom is being added to hypergraph to check and possibly
	 * add index entries for the indexed dimensions of the atom's type.
	 * </p>
	 * 
	 * @param typeHandle
	 * @param type
	 * @param atomHandle
	 * @param atom
	 */
	public void maybeIndex(HGPersistentHandle typeHandle, 
	                       HGAtomType type,
	                       HGPersistentHandle atomHandle,
	                       Object atom)
	{
		List<HGIndexer> indList = (List)indexers.get(typeHandle);
		if (indList == null)
			return;
		for (HGIndexer indexer : indList)
		{
			HGIndex<Object, Object> idx = getOrCreateIndex(indexer);
			indexer.index(graph, atomHandle, atom, idx);
//			Object key = indexer.getKey(graph, atom);			
//			Object value = (indexer instanceof HGValueIndexer) ? 
//			               ((HGValueIndexer)indexer).getValue(graph, atom) 
//						   : atomHandle;
//			idx.addEntry(key, value);
		}
	}
	
	/**
	 * <p>
	 * Called when an atom is being added to hypergraph to check and possibly
	 * add index entries for the indexed dimensions of the atom's type.
	 * </p>
	 * 
	 * @param typeHandle
	 * @param type
	 * @param atom
	 * @param atomHandle
	 */
	public void maybeUnindex(HGPersistentHandle typeHandle, 
	                         HGAtomType type,
	                         HGPersistentHandle atomHandle,
	                         Object atom)
	{
		List<HGIndexer> indList = (List)indexers.get(typeHandle);
		if (indList == null)
			return;
		for (HGIndexer indexer : indList)
		{		    
			HGIndex<Object, Object> idx = getOrCreateIndex(indexer);
			indexer.unindex(graph, atomHandle, atom, idx);
//			Object key = indexer.getKey(graph, atom);			
//			Object value = (indexer instanceof HGValueIndexer) ? ((HGValueIndexer)indexer).getValue(graph, atom) 
//						   : atomHandle;
//			idx.removeEntry(key, value);
		}
	}
}
