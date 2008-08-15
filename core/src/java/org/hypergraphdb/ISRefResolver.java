package org.hypergraphdb;

import org.hypergraphdb.handle.HGLiveHandle;
import org.hypergraphdb.storage.DBKeyedSortedSet;
import org.hypergraphdb.storage.IndexResultSet;
import org.hypergraphdb.util.ArrayBasedSet;
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
	int keepInMemoryThreshold = 10000;
	
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
				IncidenceSet result = new IncidenceSet(key, new ArrayBasedSet<HGHandle>(A));
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