package org.hypergraphdb.transaction;

import java.lang.ref.WeakReference;

import java.util.HashMap;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.hypergraphdb.cache.CacheMap;
import org.hypergraphdb.util.Pair;
import org.hypergraphdb.util.RefCountedMap;
import org.hypergraphdb.util.RefResolver;
import org.hypergraphdb.util.WeakIdentityHashMap;

public class TxCacheMap<K, V>  implements CacheMap<K, V>
{
    private RefCountedMap<K, Box> writeMap;    
    private boolean weakrefs = false;
    protected Map<K, Box> M = null;
    protected HGTransactionManager txManager;
    protected RefResolver<Object, Box> boxGetter = null;
    protected VBox<Integer> sizebox = null;
    
    public abstract class Box extends VBox<V>
    {
        public Box(HGTransactionManager txManager)
        {
            this.txManager = txManager;
            this.body = makeNewBody(null, -1, null);
        }
        
        public VBoxBody<V> getBody() { return body; }
        
        public abstract K getKey();
        
        public VBoxBody<V> commitImmediately(HGTransaction tx, V newValue, long txNumber)
        {
            return super.commit(tx, newValue, txNumber);            
        }
        
        public VBoxBody<V> commit(HGTransaction tx, V newValue, long txNumber)
        {
            if (body.version == -1)
            {
                return body = makeNewBody(newValue, tx.getNumber(), body.next);                
            }
            else
                return super.commit(tx, newValue, tx.getNumber());            
        }        
        
        @Override
        public void finish(HGTransaction tx)
        {
            if (tx.getLocalValue(this) != null)
                writeMap.remove(getKey());
            if (body.value == null && body.version == 0 && body.next == null)
                drop(getKey());
        }
    }
    
    protected class StrongBox extends Box
    {
        K key;
        
        public StrongBox(HGTransactionManager txManager, K key)
        {
            super(txManager);
            this.key = key;
        }
        
        public K getKey() { return key; } 
    }
    
    protected class WeakBox extends Box
    {
        WeakReference<K> key;
        
        public WeakBox(HGTransactionManager txManager, K key)
        {
            super(txManager);
            this.key = new WeakReference<K>(key);
        }
        
        public K getKey() { return key.get(); }
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
            
            box = writeMap.get(key);
            
            if (box != null)
            {
                M.put((K)key, box);
                return box;
            }
                        
            box = weakrefs ? new WeakBox(txManager, (K)key) : new StrongBox(txManager, (K)key);
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
        if (mapImplementation != null)
            this.weakrefs = WeakHashMap.class.isAssignableFrom(mapImplementation) ||
                            WeakIdentityHashMap.class.isAssignableFrom(mapImplementation);
        
        try
        {
            if (mapImplementation == null)
            {
                this.M = new ConcurrentHashMap<K, Box>();
            }
            else
            {
                this.M = mapImplementation.newInstance();
            }
            this.writeMap = new RefCountedMap<K, Box>(new HashMap());            
        }
        catch (Exception e)
        {
            throw new RuntimeException(e);
        }
        
        
        if (this.M instanceof ConcurrentMap)            
            boxGetter = new RefResolver<Object,Box>() 
        {
            private ConcurrentMap<K, Box> c_map = (ConcurrentMap<K, Box>)M;
            private Map<K, Box> cwrite_map = writeMap;
            public Box resolve(Object k) 
            { 
                Box box = c_map.get(k);
                if (box != null)
                    return box;
                box = cwrite_map.get(k);
                if (box != null)
                {
                    c_map.putIfAbsent((K)k, box);
                    return box;
                }
                box = weakrefs ? new WeakBox(txManager, (K)k) : new StrongBox(txManager, (K)k);
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
     * Override to maintain a global write map so that evicted versioned boxes from the cache
     * get re-attached upon reloading.
     */
    public void put(K key, V value)
    {
        HGTransaction tx = txManager.getContext().getCurrent();
        Box box = boxGetter.resolve(key);
        
        // first, check if this transaction was already written to that Box  and if not,
        // add it as a reference count
        if (tx.getLocalValue(box) == null)
            writeMap.put(key, box);
        
        V old = get(key);
        box.put(value);   // TODO maybe don't do a 'put' if value == old     
        if (old == null)
        {
            if (value != null)
                sizebox.put(sizebox.get() + 1);
        }
        else if (value == null)
        {
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
            if (tx.getNumber() >= txManager.mostRecentRecord.transactionNumber || tx == null)
            {
                // a marker that we have loaded some older values for older, but still running tx
                if (box.body.version == -1)
                {
                    box.body = box.makeNewBody(value, tx.getNumber(), box.body.next);
                }
                // otherwise we just add as the newest body with the current tx number
                else if (box.body.version == 0 || box.body.version < tx.getNumber())
                    box.commitImmediately(tx, value, tx.getNumber());
            }
            // if not current, we must insert it into the list of bodies
            else
            {               
                VBoxBody<V> currentBody = box.body;
                while (currentBody.next != null && currentBody.next.version > tx.getNumber())
                    currentBody = currentBody.next;
                currentBody.setNext(box.makeNewBody(value, tx.getNumber(), currentBody.next));                
            }
        }
        finally
        {
            txManager.COMMIT_LOCK.unlock();
        }
    }
    
    public Box boxOf(Object key)
    {
        if (M instanceof ConcurrentMap<?,?>)
            return M.get(key);
        else synchronized (M)
        {
            return M.get(key);
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
        // running. In addition, the current transaction may have started after the latest
        // value was committed or not. If the current transaction started after the latest
        // commit of this value, all we care about is whether we have that latest value in 
        // the VBox. Otherwise, we need a version that's exactly tagged with the transaction
        // number of the current transaction. This is important: for example say the current
        // transaction has number 5, and there are two versioned bodies in the box tagged
        // with 6 and 3. Clearly, we can't return version 6 because it's more recent than
        // our transaction. But we can't return version 3 either, because transaction 4 may
        // have committed an intermediate version. We don't know - version 3 might be the correct
        // one or not. So, we must force a load and tag it with version 5. In other words, we
        // must return null in this case so that the upper layers (using the cache) perform
        // a load operation.

        Box box = boxGetter.resolve(key);
               
        HGTransaction tx = txManager.getContext().getCurrent();
        
        if (tx == null)
            return box.body.value;
        
        V value = tx.getLocalValue(box);
        
        if (value == null) // no local value, we get the correct version if loaded or return null if not
        {
            VBoxBody<V> b = box.body;
            // if the current transaction is not older than the top body we return it:
            // it will be null if we need a load from disk or the latest committed value
            // which would be the correct version
            if (b.version <= tx.getNumber() && b.version != -1)
            {
                value = b.value;
                if (!tx.isReadOnly())
                    tx.bodiesRead.add(new Pair<VBox<?>, VBoxBody<?>>(box, b));                
            }
            else
            {
                // else try to find the exact same version as the current transaction
                if (b.version == -1)
                    b = b.next;
                while (b != null && b.version > tx.getNumber())
                    b = b.next;
                if (b != null && b.version == tx.getNumber())
                {
                    value = b.value;
                    if (!tx.isReadOnly())
                        tx.bodiesRead.add(new Pair<VBox<?>, VBoxBody<?>>(box, b));                    
                }
            }
        }        
        return value == HGTransaction.NULL_VALUE ? null : value;        
    }

    @SuppressWarnings("unchecked")
    public void remove(Object key)
    {
        put((K)key, null);
    }

    @SuppressWarnings("unchecked")
    public void drop(Object key)
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
        M.clear();
    }     
}