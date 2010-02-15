package org.hypergraphdb.transaction;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class TxMap<K, V> implements Map<K, V>
{
    //private ConcurrentMap<K, VBox<V>> M = new ConcurrentHashMap<K, VBox<V>>();
    private Map<K, VBox<V>> M = null;
    private HGTransactionManager txManager;
    
    @SuppressWarnings("unchecked")
    private VBox<V> getBox(Object key)
    {
        VBox<V> box = M.get(key);
        // Assume calls to this map are synchronized otherwise. If not, a ConcurrentMap needs to be 
        // used if we want lock free behavior. So the caching needs yet another rework for the
        // WeakRef maps to be lock free.
        if (box == null)
        {
            box = new VBox<V>(txManager.getContext());
            M.put((K)key, box);
        }
        return box;
        // Use this with a ConcurrentMap:
        // return (box == null) ? M.putIfAbsent((K)key, new VBox<V>(txManager.getContext())) : box;
    }
    
    public TxMap(HGTransactionManager txManager, Map<K, VBox<V>> backingMap)
    {
        this.txManager = txManager;
        this.M = backingMap;
    }
    
    public boolean containsKey(Object key)
    {
        return get(key) != null;
    }
    
    public V get(Object key)
    {
        return getBox(key).get();
    }

    public V put(K key, V value)
    {
        VBox<V> box = getBox(key);
        V old = box.get();
        box.put(value);
        return old;
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        throw new UnsupportedOperationException();
    }

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