package org.hypergraphdb.util;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 
 * <p>
 * A variation on a map where values are managed like resources: a put 
 * increments a reference count on an existing key-value entry and a remove
 * actually delete the entry when the reference count goes to zero. This
 * was implemented to help with the transactions framework.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <K>
 * @param <V>
 */
public class RefCountedMap<K, V> implements Map<K, V>
{
    Map<K, Pair<V, AtomicInteger>> implementation = null;
    
    @SuppressWarnings("unchecked")
    public RefCountedMap(Map implementation)
    {
        if (implementation == null)
            this.implementation = new ConcurrentHashMap<K, Pair<V, AtomicInteger>>();
        else
            this.implementation = (Map<K, Pair<V, AtomicInteger>>)implementation;
    }
    
    public void clear()
    {
        implementation.clear();
    }

    public boolean containsKey(Object key)
    {
        return implementation.containsKey(key);
    }

    public boolean containsValue(Object value)
    {
        return implementation.containsValue(value);
    }

    public Set<java.util.Map.Entry<K, V>> entrySet()
    {
        throw new UnsupportedOperationException();
    }

    public synchronized V get(Object key)
    {
        Pair<V, AtomicInteger> p = implementation.get(key);
        return p == null ? null : p.getFirst();
    }

    public boolean isEmpty()
    {
        return implementation.isEmpty();
    }

    public Set<K> keySet()
    {
        return implementation.keySet();
    }

    @SuppressWarnings("unchecked")
    public synchronized V put(K key, V value)
    {
        Pair<V, AtomicInteger> p = null;        
        if (implementation instanceof ConcurrentMap)
        {
            p = new Pair<V, AtomicInteger>(value, new AtomicInteger(0));
            Pair<V, AtomicInteger> existing = (Pair<V, AtomicInteger>) 
                ((ConcurrentMap)implementation).putIfAbsent(key, p);
            if (existing != null)
                p = existing;
        }
        else synchronized (implementation)
        {
            p = implementation.get(key);
            if (p == null)
            {
                p = new Pair<V, AtomicInteger>(value, new AtomicInteger(0));
                implementation.put(key, p);
            }                    
        }
        int current = p.getSecond().incrementAndGet();
        return current > 1 ? value : null;
    }

    public void putAll(Map<? extends K, ? extends V> m)
    {
        for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
            put(e.getKey(), e.getValue());
    }

    public synchronized V remove(Object key)
    {
        Pair<V, AtomicInteger> p = implementation.get(key);
        if (p == null)
            return null;
        else if (p.getSecond().decrementAndGet() == 0)
        {
            implementation.remove(key);
        }
        return p.getFirst();
    }

    public int size()
    {
        return implementation.size();
    }

    public Collection<V> values()
    {
        throw new UnsupportedOperationException();
    }

    public boolean equals(Object o)
    {
        return implementation.equals(o);
    }

    public int hashCode()
    {
        return implementation.hashCode();
    }    
}