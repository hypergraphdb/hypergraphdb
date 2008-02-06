/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.util.Iterator;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.HGQuery.hg;
import org.hypergraphdb.indexing.*;
import org.hypergraphdb.storage.BAtoHandle;

/**
 * <p>
 * The <code>HGIndexManager</code> allows you to create atom indices. Such indices
 * allow quick lookup of atoms depending on their value and/or target sets.  
 * </p>
 * <p>
 * Atom indexing in HyperGraph relies on the concept of a <code>HGIndexer</code>. Indexers
 * are always bound to an atom type. They are mainly responsible for producing an index <em>key</em> 
 * given an atom instance. The index manager interacts with the <code>HGStore</code> to create
 * and update indices at the storage level based on an implementation of the <code>HGIndexer</code>
 * interface.    
 * </p>
 * <p>
 * When an implementation of a <code>HGIndexer</code> does not produce keys of <code>byte[]</code>
 * type, it is required to return a non-null <code>ByteArrayConverter</code> from its
 * <code>getConverter</code> method. In addition, if a regular byte ordering is not appropriate
 * for a given key type, the <code>HGIndexer</code> implementation must return a non-null
 * <code>Comparator</code> from its <code>getComparator</code> method. 
 * </p>
 * <p>
 * To create a new index for a given atom type, call the <code>register(HGIndexer)</code> method.
 * To later remove it, call the <code>unregister(HGIndexer)</code> method. Registering an
 * indexer will store the indexer as a HyperGraph atom and it will request a low-level storage
 * index from the <code>HGStore</code>. All atom instance of the type for which a new indexer
 * is being registered will be automatically indexed. Naturally, this may be a very long operation.
 * Therefore it is recommended that indexer be registered at a time where there's no other activity
 * on the HyperGraph database.
 * </p>
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class HGIndexManager 
{
	private HyperGraph graph;	
	private HashMap<HGIndexer, HGIndex<? extends Object, HGPersistentHandle>> indices = 
			new HashMap<HGIndexer, HGIndex<? extends Object, HGPersistentHandle>>();	
	private HashMap<HGHandle, List<HGIndexer>> indexers = new HashMap<HGHandle, List<HGIndexer>>();
	
	private String getIndexName(HGIndexer indexer)
	{
		return graph.getPersistentHandle(indexer.getType()) + "_" + 
			   graph.getPersistentHandle(graph.getHandle(indexer));
	}
	
	private HGIndexer toAtomIndexer(HGIndexer indexer)
	{
		List<HGIndexer> L = indexers.get(indexer.getType());
		int i = L.indexOf(indexer);
		return i >= 0 ? L.get(i) : null;
	}
	
	// TODO: this needs to be made thread safe.
	private <KeyType extends Object> HGIndex<KeyType, HGPersistentHandle> getOrCreateIndex(HGIndexer indexer)
	{
		HGIndex<KeyType, HGPersistentHandle> result = (HGIndex<KeyType, HGPersistentHandle>)indices.get(indexer);
		if (result == null)
		{
			String name = getIndexName(indexer);
			result = graph.getStore().getIndex(name, 
											   indexer.getConverter(graph), 
											   BAtoHandle.getInstance(), 
											   indexer.getComparator(graph));
			if (result == null)
				result = graph.getStore().createIndex(name, 
													  indexer.getConverter(graph), 
													  BAtoHandle.getInstance(), 
													  indexer.getComparator(graph));
			indices.put(indexer, result);
		}
		return result;
	}
		
	public void deleteIndex(HGIndexer indexer)
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
			List<HGIndexer> indexerAtoms = hg.findAll(graph, hg.apply(hg.deref(graph), hg.type(indexerType)));
			for (HGIndexer indexer : indexerAtoms)
			{
				List<HGIndexer> forType = indexers.get(indexer.getType());
				if (forType == null)
				{
					forType = new ArrayList<HGIndexer>();
					indexers.put(indexer.getType(), forType);
				}
				forType.add(indexer);
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
		List<HGIndexer> forType = indexers.get(indexer.getType());
		if (forType == null)
			return false;
		int i = forType.indexOf(indexer);
		if (i < 0)
			return false;
		deleteIndex(forType.get(i));
		graph.remove(graph.getHandle(forType.get(i)));
		forType.remove(i);
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
		List<HGIndexer> forType = indexers.get(typeHandle);
		if (forType != null)
			for (Iterator<HGIndexer> i = forType.iterator(); i.hasNext(); )
			{
				HGIndexer indexer = i.next();
				deleteIndex(indexer);
				graph.remove(graph.getHandle(indexer));				
				i.remove();
			}
	}
	
	/**
	 * <p>Return <code>true</code> if the given <code>HGIndexer</code> is registered
	 * with the index manager and <code>false</code> otherwise.  
	 * @param indexer The possibly registered <code>HGIndexer</code>. 
	 */
	public boolean isRegistered(HGIndexer indexer)
	{
		List<HGIndexer> forType = indexers.get(indexer.getType());
		return forType == null ? false : forType.contains(indexer);
	}
	
	/**
	 * <p>
	 * Possibly create a new index based on the specified <code>IndexDescriptor</code>.
	 * If an index corresponding to the descriptor already exists, the method
	 * does nothing and returns <code>false</code>. Otherwise it creates a new index,
	 * which triggers the automatic indexing of all atoms of the type specified in
	 * the descriptor and returns <code>true</code> at the end.
	 * </p>
	 * 
	 * @param desc The descriptor of the index to be created.
	 * @return <code>true</code> if a new index was created and <code>false</code>
	 * otherwise.
	 */
	public <KeyType> HGIndex<KeyType, HGPersistentHandle> register(HGIndexer indexer)
	{
		List<HGIndexer> forType = indexers.get(indexer.getType());
		
		if (forType == null)
		{
			forType = new ArrayList<HGIndexer>();
			indexers.put(indexer.getType(), forType);
		}
		
		if (!forType.contains(indexer))
		{
			graph.add(indexer);			
			forType.add(indexer);
			HGIndex<KeyType, HGPersistentHandle> idx = getOrCreateIndex(indexer);
			HGSearchResult<HGPersistentHandle> rs = null;
			try
			{
				rs = graph.find(hg.type(indexer.getType()));
				while (rs.hasNext())
				{
					Object atom = graph.get(rs.next());
					idx.addEntry((KeyType)indexer.getKey(graph, atom), rs.current());
				}
			}
			finally
			{
				rs.close();
			}
			return idx;
		}
		else
		{
			return getIndex(indexer);
		}
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
	public <KeyType> HGIndex<KeyType, HGPersistentHandle> getIndex(HGIndexer indexer)
	{
		HGIndex<KeyType, HGPersistentHandle> result = (HGIndex<KeyType, HGPersistentHandle>)indices.get(indexer);
		if (result == null)
		{
			List<HGIndexer> L = indexers.get(indexer.getType());			
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
	public List<HGIndexer> getIndexersForType(HGHandle type)
	{
		return this.indexers.get(type);
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
	void maybeIndex(HGPersistentHandle typeHandle, 
					HGAtomType type,
				    HGPersistentHandle atomHandle,
				    Object atom)
	{
		List<HGIndexer> indList = (List)indexers.get(typeHandle);
		if (indList == null)
			return;
		for (HGIndexer indexer : indList)
		{
			HGIndex<Object, HGPersistentHandle> idx = getOrCreateIndex(indexer);
			Object key = indexer.getKey(graph, atom);
			idx.addEntry(key, atomHandle);
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
	void maybeUnindex(HGPersistentHandle typeHandle, 
					  HGAtomType type,
	   				  Object atom,
	   				  HGPersistentHandle atomHandle)
	{
		List<HGIndexer> indList = (List)indexers.get(typeHandle);
		if (indList == null)
			return;
		for (HGIndexer indexer : indList)
		{
			HGIndex<Object, HGPersistentHandle> idx = getOrCreateIndex(indexer);
			Object key = indexer.getKey(graph, atom);
			idx.removeEntry(key, atomHandle);
		}
	}
}