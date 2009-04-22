package org.hypergraphdb.peer;

import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.object;
import static org.hypergraphdb.peer.Structs.struct;

import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.IncidenceSet;
import org.hypergraphdb.ReadyRef;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.HGStoreSubgraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.FilterIterator;
import org.hypergraphdb.util.Mapping;
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
    
    /**
     * <p>
     * Returns a structure ready to be embedded in a message for remove transfer.
     * </p>
     * 
     * @param peer
     * @param atom
     * @param ignorePredicate A predicate mapping that allows you to specify which
     * entries to ignore. When this predicate returns true on a {@link HGPersistenceHandle},
     * the storage entry for that handle will be ignored and <code>null</code>
     * will be returned. 
     * @return
     */
    public static Object getTransferAtomRepresentation(HyperGraph graph, 
                                                       HGHandle atom)
    {
        HGPersistentHandle pHandle = graph.getPersistentHandle(atom);
        if (graph.getStore().containsLink(pHandle))
        {
            StorageGraph rawGraph = new HGStoreSubgraph(pHandle, graph.getStore());
            StorageGraph atomGraph = new AtomFilteringSubgraph(graph, rawGraph);
            Map<String, String> types = new HashMap<String, String>();
            for (Pair<HGPersistentHandle, Object> p : rawGraph)
            {
                String clname = graph.getTypeSystem().getClassNameForType(p.getFirst());
                if (clname != null)
                    types.put(p.getFirst().toString(), clname);
            }      
            return struct("storage-graph", object(atomGraph),
                          "type-classes", types);            
        }
        else
            return struct();
    }
    
    /**
     * IMPORTANT: Assumes atom does not exist locally! Writes directly to storage and updates relevant indexes
     * based on that assumptions. 
     * 
     * @param atom
     * @param graph
     * @return
     * @throws ClassNotFoundException
     */
    public static HGHandle writeTransferedAtom(final Object atom, final HyperGraph graph)
        throws ClassNotFoundException
    {
        final StorageGraph subgraph = getPart(atom, "storage-graph");
        final Map<String, String>  typeClasses = getPart(atom, "type-classes");
        final Map<HGPersistentHandle, HGPersistentHandle> substituteTypes = 
            new HashMap<HGPersistentHandle, HGPersistentHandle>();
        for (Map.Entry<String, String> e : typeClasses.entrySet())
        {
            HGPersistentHandle typeHandle = HGHandleFactory.makeHandle(e.getKey());
            String classname = e.getValue();
            if (graph.get(typeHandle) == null) // do we have the atom type locally?
            {
                    Class<?> clazz = null;
                    if(classname.startsWith("[L"))
                    {
                        classname = classname.substring(2, classname.length() - 1); //remove ending ";"
                        clazz = Array.newInstance(Thread.currentThread().getContextClassLoader().loadClass(classname), 0).getClass();
                    }
                    else
                       clazz = Thread.currentThread().getContextClassLoader().loadClass(classname);
                    HGHandle localType = graph.getTypeSystem().getTypeHandle(clazz);
                    if (localType == null)
                        throw new HGException("Unable to create local type for Java class '" + classname + "'");
                    substituteTypes.put(typeHandle, graph.getPersistentHandle(localType));
            }
        }
        // TODO - here we assume that the types don't differ, but obviously they can
        // and will in many cases. So this is a major "TDB".        
        // If something goes wrong during storing the graph and reading back
        // an atom, the following will just throw an exception and the framework
        // will reply with failure.
        return graph.getTransactionManager().transact(new Callable<HGHandle>()
        {
            public HGHandle call()
            {
                store(subgraph, graph.getStore(), substituteTypes);
                
                //
                // Update indexes:
                //
                HGIndex<HGPersistentHandle, HGPersistentHandle> indexByType = 
                                      graph.getStore().getIndex(HyperGraph.TYPES_INDEX_NAME, 
                                                                BAtoHandle.getInstance(), 
                                                                BAtoHandle.getInstance(), 
                                                                null);
                
                HGIndex<HGPersistentHandle, HGPersistentHandle> indexByValue = 
                    graph.getStore().getIndex(HyperGraph.VALUES_INDEX_NAME, 
                                              BAtoHandle.getInstance(), 
                                              BAtoHandle.getInstance(), 
                                              null);
                HGPersistentHandle [] layout = subgraph.getLink(subgraph.getRoot());
                HGPersistentHandle [] targetSet = new HGPersistentHandle[layout.length-2];
                System.arraycopy(layout, 2, targetSet, 0, layout.length-2);                
                indexByType.addEntry(layout[0], subgraph.getRoot());
                indexByValue.addEntry(layout[1], subgraph.getRoot());
                HGAtomType type = graph.get(layout[0]);
                //
                // We don't know the incidence set here, is that a problem?
                // When we are transferring larger graphs with many atoms inter-linked
                // we should make the incidences sets available. In any case, at the
                // time of this "code" writing, no types actually make use of the incidence
                // sets to create the atom values.
                Object object = type.make(layout[1], 
                                          new ReadyRef<HGHandle[]>(targetSet), 
                                          null);                
                graph.getIndexManager().maybeIndex(layout[0], 
                                                   type, 
                                                   subgraph.getRoot(), 
                                                   object);      
                                                    
                // TODO: read back the atom and refresh in cache if already cached!
                System.out.println("Stored object " +  object + " with handle " + subgraph.getRoot());
                // Add to incidence sets 
                for (HGPersistentHandle target : targetSet)
                {
                    System.out.println("Adding to incidence set of target " + target);
                    graph.getStore().addIncidenceLink(target, subgraph.getRoot());
                    IncidenceSet targetIncidenceSet = graph.getCache().getIncidenceCache().getIfLoaded(target);
                    if (targetIncidenceSet != null)
                        targetIncidenceSet.add(subgraph.getRoot());                    
                }
                return subgraph.getRoot();
            }
        });        
    }
    
    private static class AtomFilteringSubgraph implements StorageGraph
    {
        HyperGraph graph;
        StorageGraph wrapped;
        HGIndex<HGPersistentHandle, HGPersistentHandle> indexByValue;

        boolean shouldIgnore(HGPersistentHandle handle, HGPersistentHandle [] result)
        {
            if (handle.equals(wrapped.getRoot()) || result == null || result.length <= 1)
                return false;
            HGRandomAccessResult<HGPersistentHandle> rs = indexByValue.find(result[1]);
            try
            {
                return rs.goTo(handle, true) == HGRandomAccessResult.GotoResult.found;
            }
            finally
            {
                rs.close();
            }
        }
        
        AtomFilteringSubgraph(HyperGraph graph, StorageGraph wrapped)
        {
            this.wrapped = wrapped;
            indexByValue = 
                graph.getStore().getIndex(HyperGraph.VALUES_INDEX_NAME, 
                                          BAtoHandle.getInstance(), 
                                          BAtoHandle.getInstance(), 
                                          null);
            
        }

        public byte[] getData(HGPersistentHandle handle)
        {
            return wrapped.getData(handle);
        }

        public HGPersistentHandle[] getLink(HGPersistentHandle handle)
        {
            HGPersistentHandle [] result = wrapped.getLink(handle);
            return shouldIgnore(handle, result) ? null : result;
        }

        public HGPersistentHandle getRoot()
        {
            return wrapped.getRoot();
        }

        public Iterator<Pair<HGPersistentHandle, Object>> iterator()
        {
            return new FilterIterator<Pair<HGPersistentHandle, Object>>(wrapped.iterator(), 
              new Mapping<Pair<HGPersistentHandle, Object>, Boolean>()
              {
                public Boolean eval(Pair<HGPersistentHandle, Object> n)
                {
                    return n.getSecond() instanceof HGPersistentHandle[] && 
                           shouldIgnore(n.getFirst(), (HGPersistentHandle[])n.getSecond()); 
                }                                        
              }
            );            
        }        
    }
}