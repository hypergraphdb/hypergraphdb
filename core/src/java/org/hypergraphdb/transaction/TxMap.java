package org.hypergraphdb.transaction;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.hypergraphdb.util.RefResolver;

public class TxMap<K, V> implements Map<K, V>
{
    //private ConcurrentMap<K, VBox<V>> M = new ConcurrentHashMap<K, VBox<V>>();
    private Map<K, VBox<V>> M = null;
    private HGTransactionManager txManager;
    private RefResolver<Object, VBox<V>> boxGetter = null;
    
    @SuppressWarnings("unchecked")
    private synchronized VBox<V> getBox(Object key)
    {
        VBox<V> box = M.get(key);
        // Assume calls to this map are synchronized otherwise. If not, a ConcurrentMap needs to be 
        // used if we want lock free behavior. So the caching needs yet another rework for the
        // WeakRef maps to be lock free.
        if (box == null)
        {
            box = new VBox<V>(txManager);
            M.put((K)key, box);
        }
        return box;
        // Use this with a ConcurrentMap:
        // return (box == null) ? M.putIfAbsent((K)key, new VBox<V>(txManager.getContext())) : box;
    }
    
    public TxMap(HGTransactionManager tManager, Map<K, VBox<V>> backingMap)
    {
        this.txManager = tManager;
        this.M = backingMap;
        if (backingMap instanceof ConcurrentMap)            
            boxGetter = new RefResolver<Object,VBox<V>>() 
        {
            private ConcurrentMap<K, VBox<V>> cm = (ConcurrentMap<K, VBox<V>>)M;
            @SuppressWarnings("unchecked")
            public VBox<V> resolve(Object k) 
            { 
                VBox<V> box = cm.get(k);
                return (box == null) ? cm.putIfAbsent((K)k, new VBox<V>(txManager)) : box;
            }            
        };
        else
            boxGetter = new RefResolver<Object,VBox<V>>() 
            {
                public VBox<V> resolve(Object k) 
                {
                    return getBox(k);
                }            
            };
    }
    
    public boolean containsKey(Object key)
    {
        return get(key) != null;
    }
    
    public V get(Object key)
    {
        return boxGetter.resolve(key).get();
    }

    public V put(K key, V value)
    {
        VBox<V> box = boxGetter.resolve(key);
        V old = box.get();
        box.put(value);
        return old;
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    public V remove(Object key)
    {
        return put((K)key, null);
    }
    
    public boolean isEmpty()
    {
        return size() == 0;
    }

    public int size()
    {
        return M.size();
    }

    public Set<K> keySet()
    {
        return null;
    }
        
    public Collection<V> values()
    {
        return null;
    }
    
    public boolean containsValue(Object value)
    {
        throw new UnsupportedOperationException();
    }

    public Set<java.util.Map.Entry<K, V>> entrySet()
    {
        return null;
    }
    
    public void clear()
    {
        //throw new UnsupportedOperationException();
        M.clear();
    }    
}