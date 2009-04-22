package org.hypergraphdb.storage;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.util.Pair;

/**
 * <p>
 * A {@link StorageGraph} bound to a {@link HGStore}.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGStoreSubgraph implements StorageGraph
{
    private HGPersistentHandle root;
    private HGStore store;
    
    /**
     * <p>
     * Construct a new {@link HGStore} based {@link StorageGraph} 
     * </p>
     * @param root The root, starting point of the storage graph.
     * @param store The backing store instance.
     */
    public HGStoreSubgraph(HGPersistentHandle root, HGStore store)
    {
        this.root = root;
        this.store = store;
    }
    
    public byte[] getData(HGPersistentHandle handle)
    {
        return store.getData(handle);
    }

    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        return store.getLink(handle);
    }

    public HGPersistentHandle getRoot()
    {
        return root;
    }

    public Iterator<Pair<HGPersistentHandle, Object>> iterator()
    {
        return new SubgraphIterator();
    }

    private class SubgraphIterator implements Iterator<Pair<HGPersistentHandle, Object>>
    {
        LinkedList<HGPersistentHandle> remaining = new LinkedList<HGPersistentHandle>();
        HashSet<HGPersistentHandle> visited = new HashSet<HGPersistentHandle>();
        
        public SubgraphIterator()
        {
            remaining.addLast(root);
            
            //TODO some UUIDs should not be visited?
            visited.add(UUIDPersistentHandle.UUID_NULL_HANDLE);
        }
        
        public boolean hasNext()
        {
            return !remaining.isEmpty();
        }

        public Pair<HGPersistentHandle, Object> next()
        {
            Pair<HGPersistentHandle, Object> result = null;
            HGPersistentHandle h = remaining.removeFirst();
            HGPersistentHandle[] link = store.getLink(h);
           
            if (link == null)
            {
                byte [] data = store.getData(h);
                //TODO throw exception for missing data
                if (data != null) 
                {
                    visited.add(h);
                    result = new Pair<HGPersistentHandle, Object>(h, data);
                }
           }
           else
           {
               visited.add(h);
               for (HGPersistentHandle x : link)
                   if (!visited.contains(x)) remaining.addLast(x);

               result = new Pair<HGPersistentHandle, Object>(h, link);
           }

            return result;
        }

        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }    
}
