package org.hypergraphdb.transaction;

import java.lang.ref.WeakReference;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.hypergraphdb.cache.CacheMap;
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
    
    // Indicates whether the latest available version of a value should be returned.
    // This TxCacheMap is mainly designed to cache objects that are otherwise persisted
    // on this. So when something is loaded from disk in a transaction that is not the 
    // latest running transaction, it is not stored as the latest version of the value
    // (since there's no way to know if a more recent transaction has modified it). 
    // In such situations, the version element of the version linked list is set to null
    // and when somebody asks for the latest version, they will get null forcing a re-load
    // from disk. That's the correct strategy for disk-based values. But it doesn't work
    // for the reverse Object->HGHandle map in the HGDB cache because runtime instances cannot be
    // used as keys in disk lookups. For this map, we want to always return the latest available
    // version when a 'get' is made outside of a transaction or the current transaction is more recent
    // than the last version in the box linked list.
    
    private boolean returnLatestAvailable = false; 
    
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
        final K key;
        
        public StrongBox(HGTransactionManager txManager, K key)
        {
            super(txManager);
            this.key = key;
        }
        
        public K getKey() { return key; } 
    }
    
    protected class WeakBox extends Box
    {
        final WeakReference<K> key;
        
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
    public TxCacheMap(HGTransactionManager tManager, Class<? extends Map> mapImplementation, Object outer)
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
                this.writeMap = new RefCountedMap<K, Box>(new HashMap());
            }
            else
            {                
                this.M = outer == null ? mapImplementation.newInstance() : 
                    mapImplementation.getConstructor(outer.getClass()).newInstance(outer);
                this.writeMap = new RefCountedMap<K, Box>(outer == null ? mapImplementation.newInstance() : 
                    mapImplementation.getConstructor(outer.getClass()).newInstance(outer));
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
    
    public Box boxOf(Object key)
    {
        if (M instanceof ConcurrentMap<?,?>)
            return M.get(key);
        else synchronized (M)
        {
            return M.get(key);
        }
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
        txManager.COMMIT_LOCK.lock();
        try
        {
            Box box = boxGetter.resolve(key);
            HGTransaction tx = txManager.getContext().getCurrent();
            VBoxBody<V> read = null;
            if (box.body.version == -1) // version==-1 indicates latest version is not loaded
            {
                // We don't have the latest version loaded currently, so if this transaction is
                // a most recent one, set the passed in value as the latest 
                if (tx.getNumber() >= txManager.mostRecentRecord.transactionNumber || tx == null)
                {
                    box.body = box.makeNewBody(value, tx.getNumber(), box.body.next);
                    read = box.body;
                }                
                else
                {
                    // Otherwise (the transaction is not the most recent one), insert the value
                    // at the appropriate position in the body list.
                    VBoxBody<V> curr = box.body;
                    while (curr.next != null && curr.next.version > tx.getNumber())
                        curr = curr.next;
                    if (curr.next != null && curr.next.version == tx.getNumber())
                        curr.next.value = value;
                    else
                        curr.setNext(box.makeNewBody(value, tx.getNumber(), curr.next));
                    read = curr.next;
                }
            }
            else
            {   
                // box.body.version is already the latest, so just update the value if we're loading
                // ...useful if this is gc-ed weak ref
                if (tx.getNumber() >= box.body.version)
                {
                    box.body.value = value;
                    read = box.body;                    
                }
                else
                {                    
                    VBoxBody<V> curr = box.body;
                    while (curr.next != null && curr.next.version > tx.getNumber())
                        curr = curr.next;
                    if (curr.next != null && curr.next.version == tx.getNumber())
                    {
                        curr.next.value = value;
                        read = curr.next;
                    }
                    else
                    {
                        read = box.makeNewBody(value, tx.getNumber(), curr.next);
                        curr.setNext(read);
                    }
                }
            }   
            if (!tx.isReadOnly())
                tx.bodiesRead.put(box, read);
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
        // a load operation. That is, unless the returnLatestAvailable flag is set in which
    	// case we just have to return the latest available version.

        Box box = boxGetter.resolve(key);
               
        HGTransaction tx = txManager.getContext().getCurrent();
        
        if (tx == null)
        {
        	if (returnLatestAvailable)
        		for (VBoxBody<V> body = box.body; body != null; body = body.next)
        			if (body.value != null)
        				return body.value;
            return box.body.value;
        }
        
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
                    tx.bodiesRead.put(box, b);                
            }
            else
            {
                // else try to find the exact same version as the current transaction
                if (b.version == -1)
                    b = b.next;
                // If the transaction is not readonly and we are not reading the latest committed
                // value, it will conflict at the end anyway, so we might as well cut it off immediately.
                // IMPORTANT NOTE: even though this looks like an optimization, without it the DataTxTests 
                // fails and I haven't been able to figure out way after weeks of intense debugging, so
                // I concluded it *could* have to do with a BerkeleyDB issue. What the following statement is
                // essentially doing is preventing a read from disk on any dirty data in non-read-only 
                // transactions. While debugging, I had modified the 'load' method to actually not load
                // but throw the same TransactionConflictException in the exact same situation, but that
                // didn't prevent the test from failing. So even though the cache is kept intact when 
                // an older transaction tries to get old data and it's not read-only, some dirty data still
                // sneaks in as latest as soon as it is being read from disk. I could not find a way to 
                // explain way short of blaming BerkleyDB. At least the tests pass with the below. --Boris 
                else if (!tx.isReadOnly())
                    throw new TransactionConflictException();
                while (b != null && b.version > tx.getNumber())
                    b = b.next;
                if (b != null && b.version == tx.getNumber())
                {                    
                    value = b.value;
                    if (!tx.isReadOnly())
                    {
                        tx.bodiesRead.put(box, b);
                    }
                }                
            }
        }
        
        if (value == null && returnLatestAvailable)
    		for (VBoxBody<V> body = box.body; body != null; body = body.next)
    			if (body.value != null)
    				value = body.value;
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
    
    public Set<K> keySet() { return M.keySet(); }

	public boolean isReturnLatestAvailable()
	{
		return returnLatestAvailable;
	}

	public void setReturnLatestAvailable(boolean returnLatestAvailable)
	{
		this.returnLatestAvailable = returnLatestAvailable;
	}   
}