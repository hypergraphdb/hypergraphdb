package org.hypergraphdb.peer;

import java.util.HashMap;
import java.util.Map;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.util.Pair;

/**
 * @author ciprian.costa
 * 
 *         Some generic operations that can be done with subgraphs (like adding
 *         one to a HGDB)
 */
public class SubgraphManager
{
    public static void store(StorageGraph subgraph, HGStore store)
    {
        store(subgraph, store, new HashMap<HGPersistentHandle, HGPersistentHandle>());
    }
    
    public static void store(StorageGraph subgraph, 
                             HGStore store, 
                             Map<HGPersistentHandle, HGPersistentHandle> substitute)
    {
        for (Pair<HGPersistentHandle, Object> item : subgraph)
        {
            if (substitute.containsKey(item.getFirst())) // local entry existing, skip...
                continue;            
            // TODO should make sure the handle is not already in there?
            if (item.getSecond() instanceof byte[])
                store.store(item.getFirst(), (byte[]) item.getSecond());
            else
            {
                HGPersistentHandle [] layout = (HGPersistentHandle[]) item.getSecond();
                for (int i = 0; i < layout.length; i++)
                {
                    HGPersistentHandle h = substitute.get(layout[i]);
                    if (h != null)
                        layout[i] = h;                    
                }
                store.store(item.getFirst(), layout);
            }
        }
    }

    public static Object get(StorageGraph subgraph, HyperGraph graph)
    {
        store(subgraph, graph.getStore());
        Object result = graph.get(subgraph.getRoot());
        graph.remove(subgraph.getRoot());
        return result;
    }
}