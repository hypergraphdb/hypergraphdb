package org.hypergraphdb.cache;

import java.util.HashMap;

public class HashCacheMap<K, V> implements CacheMap<K, V>
{
    private HashMap<K, V> m = new HashMap<K, V>();
    
    public V get(K key)
    {
        return m.get(key);
    }

    public void load(K key, V value)
    {
        put(key, value);
    }

    public void put(K key, V value)
    {
        m.put(key, value);
    }

    public void remove(K key)
    {
        m.remove(key);
    }

    public void drop(K key)
    {
        m.remove(key);
    }
    
    public void clear()
    {
        m.clear();
    }
    
    public int size()
    {
        return m.size();
    }
}