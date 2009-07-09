package org.hypergraphdb.peer;

import static org.hypergraphdb.peer.Structs.getPart;
import static org.hypergraphdb.peer.Structs.object;
import static org.hypergraphdb.peer.Structs.struct;

import java.io.File;
import java.io.FileWriter;
import java.lang.reflect.Array;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.ReadyRef;
import org.hypergraphdb.algorithms.CopyGraphTraversal;
import org.hypergraphdb.algorithms.HGTraversal;
import org.hypergraphdb.algorithms.HyperTraversal;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.HGStoreSubgraph;
import org.hypergraphdb.storage.RAMStorageGraph;
import org.hypergraphdb.storage.StorageGraph;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.util.FilterIterator;
import org.hypergraphdb.util.HGUtils;
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
    /**
     * Debugging method: will write the complete StorageGraph in a File where lines
     * are sorted by the graph's key handles.
     * 
     * @param file
     */
    public static void dumpGraphToFile(File file, StorageGraph graph, boolean outputByteBuffers)
    {
        try
        {
            FileWriter out = new FileWriter(file);
            TreeMap<HGPersistentHandle, Object> sorted = new TreeMap<HGPersistentHandle, Object>();
            for (Pair<HGPersistentHandle, Object> p : graph)
                sorted.put(p.getFirst(), p.getSecond());
            for (HGPersistentHandle h : sorted.keySet())
            {
                out.write(h.toString() + "=[");
                Object x = sorted.get(h);
                if (x instanceof HGPersistentHandle [])
                {
                    HGPersistentHandle [] link = (HGPersistentHandle[])x;
                    for (int i = 0; i < link.length; i++)
                    {
                        out.write(link[i].toString());
                        if (i < link.length - 1)
                            out.write(",");
                    }
                }
                else
                {
                    byte [] A = (byte[])x;
                    if (outputByteBuffers)
                    {
                        for (int i = 0; i < A.length; i++)
                        {
                            out.write(Byte.toString(A[i]));
                            if (i < A.length - 1)
                                out.write(",");                            
                        }
                    }
                    else
                    {
                        out.write("byte[]");
                    }
                }   
                out.write("]\n");
            }
            out.close();
        }
        catch (Exception ex)
        {
            System.err.println(ex);
        }
    }
    
    public static void store(StorageGraph subgraph, HGStore store)
    {
        store(subgraph, store, new HashMap<HGPersistentHandle, HGPersistentHandle>());
    }
    
    /**
     * <p>
     * Write the passed in {@link StorageGraph} to the permanent
     * <code>HGStore</code> in a single transaction. The <code>StorageGraph</code>
     * must be small enough to fit in a single transaction. How small exactly
     * that is depends on the underlying storage mechanism, available memory,
     * available transactional locks etc. 
     * </p>
     * 
     * <p>
     * You can specify a map from data in the <code>StorageGraph</code> to "local"
     * <code>HGStore</code> data in the <code>substitute</code> parameter. This map 
     * states that a given key is to replaced by the given value during storage. Elements
     * in this map's domain are not written because they have equivalents in the 
     * <code>HGStore</code> already. Moreover, whenever a link from the <code>StorageGraph</code>
     * parameter points to one of the elements in map's domain it is replaced by the
     * corresponding element of the map's range.
     * </p>
     * 
     */
    public static void store(final StorageGraph subgraph, 
                             final HGStore store, 
                             final Map<HGPersistentHandle, HGPersistentHandle> substitute)
    {        
    
        store.getTransactionManager().transact(new Callable<Object>()
        {
           public Object call() 
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
                return null;
           }
        });
    }

    /**
     * Assuming a single root, write the <code>StorageGraph</code> to (merge it with)
     * the <code>HyperGraph</code> for the sole purpose of reading back a run-time
     * atom. Then delete that atom from the graph. 
     * 
     * <strong>NOTE: this method assumes that the single atom represented in the StorageGraph
     * parameter does not exist locally in the HyperGraph.</strong>
     * 
     * @param subgraph
     * @param graph
     * @return
     */
    public static Object get(StorageGraph subgraph, HyperGraph graph)
    {
        store(subgraph, graph.getStore());
        HGPersistentHandle theRoot = subgraph.getRoots().iterator().next();
        Object result = graph.get(theRoot);
        graph.remove(theRoot);
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
    
    public static Object getTransferGraphRepresentation(HyperGraph graph,
                                                        HGTraversal traversal)
    {
        Set<HGPersistentHandle> roots = new HashSet<HGPersistentHandle>();
        CopyGraphTraversal copyTraversal = null;
        if (traversal instanceof CopyGraphTraversal)
            copyTraversal = (CopyGraphTraversal)traversal;
        else if (traversal instanceof HyperTraversal)
            copyTraversal = (CopyGraphTraversal)((HyperTraversal)traversal).getFlatTraversal();
        else
            throw new RuntimeException("Expecting a CopyGraphTraversal or a HyperTraversal.");        
        roots.add(graph.getPersistentHandle(copyTraversal.getStartAtom()));
        while (traversal.hasNext())
        {            
            Pair<HGHandle, HGHandle> link = traversal.next();
            roots.add(graph.getPersistentHandle(link.getFirst()));
            roots.add(graph.getPersistentHandle(link.getSecond()));
        }
        StorageGraph rawGraph = new HGStoreSubgraph(roots, graph.getStore());
        StorageGraph atomGraph = rawGraph; //new AtomFilteringSubgraph(graph, rawGraph);
        Map<String, String> types = new HashMap<String, String>();
        for (Pair<HGPersistentHandle, Object> p : rawGraph)
        {
/*        	if (graph == null)
        		throw new NullPointerException("graph is null");
        	else if (graph.getTypeSystem() == null)
        		throw new NullPointerException("graph type system is null is null");
        	else if (p == null)
        		throw new NullPointerException("p is null is null");
        	else if (p.getFirst() == null)
        		throw new NullPointerException("p.getFirst is null"); */
            String clname = graph.getTypeSystem().getClassNameForType(p.getFirst());
            if (clname != null)
                types.put(p.getFirst().toString(), clname);
        }
        return struct("storage-graph", object(atomGraph),
                      "type-classes", types);         
    }
    
    private static void translateBatch(HyperGraph graph, 
                                       Set<HGPersistentHandle> batch, 
                                       RAMStorageGraph subgraph,
                                       Map<HGPersistentHandle, Object> objects,
                                       Mapping<Pair<HGPersistentHandle, Object>, HGPersistentHandle> atomFinder,
                                       Map<HGPersistentHandle, HGPersistentHandle> substitutes)
    {
        for (HGPersistentHandle atom : batch)
        {
            HGPersistentHandle [] layout = subgraph.getLink(atom);                
            Object object = null;
            graph.getStore().attachOverlayGraph(subgraph);
            try
            {    
                HGHandle [] targetSet = new HGHandle[layout.length-2];
                System.arraycopy(layout, 2, targetSet, 0, layout.length-2);                                                             
                HGAtomType type = graph.get(layout[0]);                    
                object = type.make(layout[1], 
                                   new ReadyRef<HGHandle[]>(targetSet), 
                                   null);                
            }
            finally
            {
                graph.getStore().detachOverlayGraph();
            }            
            HGPersistentHandle existing = atomFinder == null ? null : 
                atomFinder.eval(new Pair<HGPersistentHandle, Object>(atom, object));
            if (existing != null)
                substitutes.put(atom, existing);
            else 
                objects.put(atom, object);                    
        }        
    }
    
    private static Set<HGPersistentHandle> translateAtoms(final HyperGraph graph, 
                                                          final RAMStorageGraph subgraph,
                                                          final Map<HGPersistentHandle, Object> objects,
                                                          final Mapping<Pair<HGPersistentHandle, Object>, 
                                                                       HGPersistentHandle> atomFinder)
    {
        final Map<HGPersistentHandle, HGPersistentHandle> substitutes = 
            new HashMap<HGPersistentHandle, HGPersistentHandle>();
        final Set<HGPersistentHandle> batch = new HashSet<HGPersistentHandle>();
        for (HGPersistentHandle theRoot : subgraph.getRoots())
        {
            batch.add(theRoot);            
            if (batch.size() < 200)
            {
                continue;
            }
            else 
            {
                graph.getTransactionManager().transact(new Callable<Object>() {
                public Object call()
                {
                   translateBatch(graph, batch, subgraph, objects, atomFinder, substitutes);
                   batch.clear();
                   return null; 
                }
                });
            }
        }
        graph.getTransactionManager().transact(new Callable<Object>() {
            public Object call()
            {
                translateBatch(graph, batch, subgraph, objects, atomFinder, substitutes);
                return null; 
            }
            });        
        subgraph.translateHandles(substitutes);
        return substitutes.keySet();
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
    public static Set<HGHandle> writeTransferedGraph(final Object atom, 
                                                     final HyperGraph graph)
        throws ClassNotFoundException
    {
        return writeTransferedGraph(atom, graph, null);
    }
    
    /**
     * @param atom A Structs representation of a transfered graph.
     * @param graph The HyperGraph instance to be written to.
     * @param atomFinder Return handles of existing atoms so that they are not stored
     * again or under a different handle. This parameter can be null in which
     * case existing atoms (with the same handle) are overwritten and equivalents
     * (same "information import", but different handle) are ignored.
     * 
     * @return The set of atoms that where stored.
     * @throws ClassNotFoundException
     */
    public static Set<HGHandle> writeTransferedGraph(final Object atom, 
                                                     final HyperGraph graph,
                                                     final Mapping<Pair<HGPersistentHandle, Object>, HGPersistentHandle> atomFinder)
        throws ClassNotFoundException
    {
        // TODO - here we assume that the types don't differ, but obviously they can
        // and will in many cases. So this is a major "TDB".        
        // If something goes wrong during storing the graph and reading back
        // an atom, the following will just throw an exception and the framework
        // will reply with failure.
        
        final RAMStorageGraph subgraph = getPart(atom, "storage-graph");        
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
                        clazz = Array.newInstance(HGUtils.loadClass(graph, classname), 0).getClass();
                    }
                    else
                       clazz = HGUtils.loadClass(graph, classname);
                    HGHandle localType = graph.getTypeSystem().getTypeHandle(clazz);
                    if (localType == null)
                        throw new HGException("Unable to create local type for Java class '" + classname + "'");
                    substituteTypes.put(typeHandle, graph.getPersistentHandle(localType));
            }
        }      
        subgraph.translateHandles(substituteTypes);        
         
        Map<HGPersistentHandle, Object> objects = new HashMap<HGPersistentHandle, Object>();
        Set<HGPersistentHandle> ignoreAtoms =  translateAtoms(graph, subgraph, objects, atomFinder);
                       
        return storeObjectsTransaction(graph, subgraph, objects, ignoreAtoms);        
    }
    
    private static Set<HGHandle> storeObjectsTransaction(final HyperGraph graph,
                                                         final RAMStorageGraph subgraph,
                                                         final Map<HGPersistentHandle, Object> objects,
                                                         final Set<HGPersistentHandle> ignoreAtoms)
    {
        Set<HGHandle> result = new HashSet<HGHandle>();
        final Set<HGPersistentHandle> batch = new HashSet<HGPersistentHandle>();
        for (HGPersistentHandle theRoot : subgraph.getRoots())
        {
            if (ignoreAtoms.contains(theRoot))
                continue;
            else
                batch.add(theRoot);
            if (batch.size() == 200)
            {
                graph.getTransactionManager().transact(new Callable<Object>() {
                public Object call()
                {
                    for (HGPersistentHandle atom : batch)
                    {
                        HGPersistentHandle [] layout = subgraph.getLink(atom);
                        Object x = objects.get(atom);
                        if (layout.length > 2) // it's a link, need to port local handle translation to the object
                            for (int i = 2; i < layout.length; i++)
                                ((HGLink)x).notifyTargetHandleUpdate(i-2, layout[i]);
                        graph.define(atom, 
                                     layout[0],                                 
                                     x,
                                     (byte)0);
                    }
                    return null;                
                }});
                result.addAll(batch);
                batch.clear();
            }
        }
        graph.getTransactionManager().transact(new Callable<Object>() {
            public Object call()
            {
                for (HGPersistentHandle atom : batch)
                {
                    HGPersistentHandle [] layout = subgraph.getLink(atom);
                    Object x = objects.get(atom);
                    if (layout.length > 2) // it's a link, need to port local handle translation to the object
                        for (int i = 2; i < layout.length; i++)
                            ((HGLink)x).notifyTargetHandleUpdate(i-2, layout[i]);                    
                    graph.define(atom, 
                                 layout[0],                                 
                                 x,
                                 (byte)0);
                }
                return null;                
            }});
        result.addAll(batch);        
        return result;
    }
    
    /**
     * A StorageGraph that returns atoms only - i.e., getLink will return null
     * for everything that is NOT an atom and the iterator will return a stream
     * of atoms only.
     * 
     * Something is an atom if it's either part of the "roots" set of the underlying
     * StorageGraph or if it has a value handle in a standard [type, value, ...] pointing
     * to it through the HGDB value index.
     * 
     * @author Borislav Iordanov
     *
     */
    private static class AtomFilteringSubgraph implements StorageGraph
    {
        HyperGraph graph;
        StorageGraph wrapped;
        HGIndex<HGPersistentHandle, HGPersistentHandle> indexByValue;

        boolean shouldIgnore(HGPersistentHandle handle, HGPersistentHandle [] result)
        {
            if (wrapped.getRoots().contains(handle) || result == null || result.length <= 1)
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

        public Set<HGPersistentHandle> getRoots()
        {
            return wrapped.getRoots();
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