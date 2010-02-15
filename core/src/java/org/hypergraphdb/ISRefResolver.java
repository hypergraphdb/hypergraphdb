/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb;

import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.storage.DBKeyedSortedSet;
import org.hypergraphdb.storage.IndexResultSet;
import org.hypergraphdb.transaction.TxSet;
import org.hypergraphdb.util.ArrayBasedSet;
import org.hypergraphdb.util.DummyReadWriteLock;
import org.hypergraphdb.util.HGLock;
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
	int keepInMemoryThreshold = 100000;
	
	ISRefResolver(HyperGraph graph)
	{
		this.graph = graph;
	}

	public IncidenceSet resolve(HGPersistentHandle key)
	{
		HGSearchResult<HGPersistentHandle> rs = graph.getStore().getIncidenceResultSet(key);
		try
		{
			int size = rs == HGSearchResult.EMPTY ? 0 : ((IndexResultSet<HGPersistentHandle>)rs).count();
			if (size < keepInMemoryThreshold)
			{
				HGPersistentHandle [] A = new HGPersistentHandle[size];
				for (int i = 0; i < A.length; i++)
					A[i] = rs.next();
				
				ArrayBasedSet<HGHandle> impl = new ArrayBasedSet<HGHandle>(A);
//				impl.setLock(new HGLock(graph, key.toByteArray()));
                impl.setLock(new DummyReadWriteLock());
				
				IncidenceSet result = new IncidenceSet(key, new TxSet(graph.getTransactionManager(), impl));
				HGLiveHandle lHandle = graph.cache.get(key);
				if (lHandle != null)
					graph.updateLinksInIncidenceSet(result, lHandle);
				return result;
			}
			else
				return new IncidenceSet(key, 
										new DBKeyedSortedSet(graph.getStore().getIncidenceDbAsIndex(), key));
		}
		finally
		{
			rs.close();
		}
	}	
}
