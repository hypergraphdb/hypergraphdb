/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see
 * the LicensingInformation file at the root level of the distribution.
 *
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb;

import java.util.ArrayList;

import org.hypergraphdb.handle.HGLiveHandle;

import org.hypergraphdb.storage.StorageBasedIncidenceSet;
import org.hypergraphdb.transaction.TxCacheSet;
import org.hypergraphdb.transaction.TxSet.SetTxBox;
import org.hypergraphdb.util.ArrayBasedSet;
import org.hypergraphdb.util.CountMe;
import org.hypergraphdb.util.DummyReadWriteLock;
import org.hypergraphdb.util.HGSortedSet;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.RefCountedMap;
import org.hypergraphdb.util.RefResolver;

/**
 *
 * <p>
 * Support for incidence set caching - a reference resolver of incidence sets
 * keyed by their atom handles. Used in the implementation of incidence set caches.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
@SuppressWarnings("unchecked")
class ISRefResolver implements RefResolver<HGPersistentHandle, IncidenceSet>
{
	HyperGraph graph;
	int keepInMemoryThreshold;
	RefCountedMap<HGPersistentHandle, SetTxBox<HGHandle>> writeMap;

	RefResolver<HGPersistentHandle, HGSortedSet<HGHandle>> loader =
		new RefResolver<HGPersistentHandle, HGSortedSet<HGHandle>>()
		{
			public HGSortedSet<HGHandle> resolve(HGPersistentHandle key)
			{
				HGSearchResult<HGPersistentHandle> rs = graph.getStore().getIncidenceResultSet(key);
				try
				{
					int size = rs == HGSearchResult.EMPTY ? 0 : ((CountMe)rs).count();
					HGPersistentHandle [] A = new HGPersistentHandle[size];
					for (int i = 0; i < A.length; i++)
						A[i] = rs.next();
					ArrayBasedSet<HGHandle> impl = new ArrayBasedSet<HGHandle>(A);
					impl.setLock(new DummyReadWriteLock());
					return impl;
				}
				finally
				{
					rs.close();
				}
			}
	};

	ISRefResolver(HyperGraph graph)
	{
		this.graph = graph;
		this.keepInMemoryThreshold = graph.getConfig().getMaxCachedIncidenceSetSize();
		this.writeMap = new RefCountedMap<HGPersistentHandle, SetTxBox<HGHandle>>(null);
	}

	public IncidenceSet resolve(HGPersistentHandle key)
	{
		HGSearchResult<HGPersistentHandle> rs = graph.getStore().getIncidenceResultSet(key);
		try
		{
			int size = keepInMemoryThreshold;
			if (keepInMemoryThreshold < Integer.MAX_VALUE)
				size = rs == HGSearchResult.EMPTY ? 0 : ((CountMe)rs).count();
			if (size <= keepInMemoryThreshold)
			{
				ArrayList<HGPersistentHandle> A = new ArrayList<HGPersistentHandle>();
				while (rs.hasNext())
					A.add(rs.next());
				ArrayBasedSet<HGHandle> impl = new ArrayBasedSet<HGHandle>(A.toArray(HGUtils.EMPTY_HANDLE_ARRAY));
				impl.setLock(new DummyReadWriteLock());
				IncidenceSet result = new IncidenceSet(key,
														new TxCacheSet(graph.getTransactionManager(),
																					 impl,
																					 key,
																					 loader,
																					 writeMap));
				HGLiveHandle lHandle = graph.cache.get(key);
				if (lHandle != null)
					graph.updateLinksInIncidenceSet(result, lHandle);
				return result;
			}
			else
				return new IncidenceSet(key, new StorageBasedIncidenceSet(key, graph));
		}
		finally
		{
			rs.close();
		}
	}
}