package org.hypergraphdb.peer;

import java.util.Iterator;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.util.Pair;

/**
 * @author ciprian.costa
 *
 *  Some generic operations that can be done with subgraphs (like adding one
 *  to a HGDB)
 */
public class SubgraphManager
{
	public static HGHandle store(Subgraph subgraph, HGStore store)
	{
		HGHandle handle = null;
		
		Iterator<Pair<HGPersistentHandle, Object>> iter = subgraph.iterator();
		while(iter.hasNext())
		{
			Pair<HGPersistentHandle, Object> item = iter.next();

			//return the first handle 
			if (handle == null) handle = item.getFirst();
			
			//TODO should make sure the handle is not already in there? 
			if (item.getSecond() instanceof byte[])
			{
				store.store(item.getFirst(), (byte[])item.getSecond());
			}else{
				store.store(item.getFirst(), (HGPersistentHandle[])item.getSecond());
			}
		}
		
		return handle;
	}
	
	public static Object get(Subgraph subgraph, HyperGraph graph)
	{
		HGHandle handle = store(subgraph, graph.getStore());
		
		Object result = graph.get(handle);
		
		graph.remove(handle);
		
		return result;
		
	}
}
