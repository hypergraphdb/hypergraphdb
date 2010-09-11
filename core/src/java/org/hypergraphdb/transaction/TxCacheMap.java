package org.hypergraphdb.transaction;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.hypergraphdb.cache.CacheMap;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.RefResolver;

public class TxCacheMap<K, V>  implements CacheMap<K, V>
{
    private Map<K, Pair<Box, AtomicInteger>> writeMap;    
    
    //private ConcurrentMap<K, VBox<V>> M = new ConcurrentHashMap<K, VBox<V>>();
    protected Map<K, Box> M = null;
    protected HGTransactionManager txManager;
    protected RefResolver<Object, Box> boxGetter = null;
    protected VBox<Integer> sizebox = null;
    
    protected class Box extends VBox<V>
    {
        K key;
        
        public Box(HGTransactionManager txManager, K key)
        {
            super(txManager);
            this.key = key;
        }
        
        public VBoxBody<V> commit(HGTransaction tx, V newValue, long txNumber)
        {
            if (M instanceof ConcurrentMap<?, ?>)
            {
                // is this right? no synchronization here?
                if (writeMap.get(key).getSecond().decrementAndGet() == 0)
                    writeMap.remove(key);
            }
            else synchronized (M)
            {
                if (writeMap.get(key).getSecond().decrementAndGet() == 0)
                    writeMap.remove(key);
            }
            if (body.value == null)
                return body = makeNewBody(newValue, tx.getNumber(), body.next);
            else // otherwise we just add as the newest body with the current tx number
                return super.commit(tx, newValue, tx.getNumber());            
        }
    }
    
    @SuppressWarnings("unchecked")
    protected Box getBox(Object key)
    {
        synchronized (M)
        {
            Box box = M.get(key);
            // Assume calls to this map are synchronized otherwise. If not, a ConcurrentMap needs to be 
            // used if we want lock free behavior. So the caching needs yet another rework for the
            // WeakRef maps to be lock free.
            if (box != null)
                return box;
            
            Pair<Box, AtomicInteger> p = writeMap.get(key);
            
            if (p != null)
            {
                M.put((K)key, p.getFirst());
                return p.getFirst();
            }
            
            box = new Box(txManager, (K)key);
            M.put((K)key, box);
            return box;
        }
    }
    
    @SuppressWarnings("unchecked")
    public TxCacheMap(HGTransactionManager tManager, Class<? extends Map> mapImplementation)
    {
        this.txManager = tManager;
        this.sizebox = new VBox<Integer>(txManager);
        this.sizebox.put(0);
        
        try
        {
            if (mapImplementation == null)
            {
                this.M = new ConcurrentHashMap<K, Box>();
                this.writeMap = new ConcurrentHashMap<K, Pair<Box, AtomicInteger>>();
            }
            else
            {
                this.M = mapImplementation.newInstance();
                writeMap = mapImplementation.newInstance();
            }
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        
        if (this.M instanceof ConcurrentMap)            
            boxGetter = new RefResolver<Object,Box>() 
        {
            private ConcurrentMap<K, Box> c_map = (ConcurrentMap<K, Box>)M;
            private ConcurrentMap<K, Pair<Box, AtomicInteger>> cwrite_map = 
                (ConcurrentMap<K, Pair<Box, AtomicInteger>>)writeMap;
            public Box resolve(Object k) 
            { 
                Box box = c_map.get(k);
                if (box != null)
                    return box;
                Pair<Box, AtomicInteger> p = cwrite_map.get(k);
                if (p != null)
                {
                    c_map.putIfAbsent((K)k, p.getFirst());
                    return p.getFirst();
                }
                box = new Box(txManager, (K)k);
                Box box2 = c_map.putIfAbsent((K)k, box);
                return box2 != null ? box2 : box;
            }            
        };
        else
            boxGetter = new RefResolver<Object,Box>() 
            {
                public Box resolve(Object k) 
                {
                    return getBox(k);
                }            
            };        
    }
    
    /**
     * Overriden to maintain a global write map so that evicted versioned boxes from the cache
     * get re-attached upon reloading.
     */
    @SuppressWarnings("unchecked")
    public void put(K key, V value)
    {
        HGTransaction tx = txManager.getContext().getCurrent();        
        Pair<Box, AtomicInteger> p = null; // writeMap pair
        Box box = boxGetter.resolve(key);
        
        // first, check if this transaction was already written to that Box  and if not,
        // add it as a reference count
        if (tx.getLocalValue(box) == null)
        {
            if (writeMap instanceof ConcurrentMap)
            {
                p = (Pair<Box, AtomicInteger>) ((ConcurrentMap)writeMap).putIfAbsent(key, 
                        new Pair<Box, AtomicInteger>(box, new AtomicInteger(0)));                
            }
            else synchronized (M)
            {
                p = writeMap.get(key);
                if (p == null)
                {
                    p = new Pair<Box, AtomicInteger>(box, new AtomicInteger(0));
                    writeMap.put(key, p);
                }                    
            }
            p.getSecond().incrementAndGet();
        }
        
        V old = box.get();
        if (old == null)
        {
            box.put(value);
            if (value != null)
                sizebox.put(sizebox.get() + 1);
        }
        else if (old != value)
        {
            box.put(value);
            if (value == null)
                sizebox.put(sizebox.get() - 1);
        }
    }
    
    public void load(K key, V value)
    {
        Box box = boxGetter.resolve(key);
        HGTransaction tx = txManager.getContext().getCurrent();
        txManager.COMMIT_LOCK.lock();
        try
        {
            // if no current transaction, assume the value is the latest...no way to find out
            if (tx == null || tx.getNumber() >= txManager.mostRecentRecord.transactionNumber)
            {
                // a marker that we have loaded some older values for older, but still running tx
                if (box.body.value == null)
                {
                    box.body = box.makeNewBody(value, tx.getNumber(), box.body.next);
                }
                else // otherwise we just add as the newest body with the current tx number
                    box.commit(tx, value, tx.getNumber());
            }
            // if not current, we must insert it into the list of bodies and make sure
            // the top body is "null" in order to force a subsequent reload
            else
            {
                // make sure top body is null
                if (box.body.value != null)
                    box.commit(tx, null, txManager.mostRecentRecord.transactionNumber);
                
                VBoxBody<V> currentBody = box.body;
                while (currentBody.next != null && currentBody.next.version > tx.getNumber())
                    currentBody = currentBody.next;
                // we need to insert b/w currentBody and currentBody.next
                VBoxBody<V> newBody = box.makeNewBody(value, tx.getNumber(), currentBody.next);
                currentBody.setNext(newBody);                
            }
        }
        finally
        {
            txManager.COMMIT_LOCK.unlock();
        }
    }
    
    public V get(Object key)
    {        
        // The logic to get the correct version here is a bit different than
        // the normal VBox.get. Clearly, a tx local value is returned first, if any.
        // But if there's no local value, we must make sure the correct version is returned.
        // There are several cases to consider. Because of cache eviction patterns and 
        // potentially long running transactions, we may have the latest committed value
        // in the VBox or we may just have old values loaded for older transactions still
        // running. In addition the current transaction may have started after the latest
        // value was committed or not. If the current transaction started after the latest
        // commit of this value, all we care about is whether we have that latest value in 
        // the VBox. Otherwise, we need a version that's exactly tagged with the transaction
        // number of the current transaction. This is important: for example say the current
        // transaction has number 5, and there are two versioned bodies in the box tagged
        // with 6 and 3. Clearly, we can't return version 6 because it's more recent than
        // our transaction. But we can't return version 3 either, because transaction 4 may
        // have committed an intermediate version. We don't know - version 3 might be the correct
        // one or not. So, we must force a load and tag it with version 4. In other words, we
        // must return null in this case so that the upper layers (using the cache) perform
        // a load operation.
 
        Box box = boxGetter.resolve(key);
        
        HGTransaction tx = txManager.getContext().getCurrent();
        
        V value = tx.getLocalValue(box);
        
        if (value == null) // no local value, we get the correct version if loaded or return null if not
        {
            VBoxBody<V> b = box.body;
            // if the current transaction is not older than the top body we return it:
            // it will be null if we need a load from disk or the latest committed value
            // which would be the correct version
            if (b.version <= tx.getNumber())
                value = b.value;
            else
            {
                // else try to find the exact same version as the current transaction                
                while (b.version > tx.getNumber() && b.next != null)
                    b = b.next;
                if (b.version == tx.getNumber())
                    value = b.value;
            }
        }        
        return value == HGTransaction.NULL_VALUE ? null : value;
    }

    /**
     * This method will always return null - it's purpose is to evict something from the cache
     * so the value that's already in there shouldn't matter!
     */
    @SuppressWarnings("unchecked")
    public void remove(Object key)
    {
        if (M instanceof ConcurrentMap)
            M.remove(key);
        else synchronized (M) { M.remove((K)key); }
    }
    
    public int mapSize()
    {
        return M.size();
    }
    
    public int size()
    {
        return sizebox.get();
    }
    
    public void clear()
    {
        //throw new UnsupportedOperationException();
        M.clear();
    }     
}