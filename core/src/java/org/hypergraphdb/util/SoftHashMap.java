/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * 
 * <p>
 * This is similar to the standard <code>WeakHashMap</code>, but it 
 * uses <code>SoftReference</code>s for the map's values instead 
 * of <code>WeakReference</code>s for the maps keys. Thus, an entry is removed
 * from the map when the JVM is low in memory and when the entry's value is
 * softly reachable. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <K> The type of the map key.
 * @param <V> The type of the map value.
 */
@SuppressWarnings("unchecked")
public class SoftHashMap <K, V> extends AbstractMap<K, V>
{
    private static class SoftValue<Ka, Va> extends SoftReference<Va> 
    {
        @SuppressWarnings("unused")
        private final Ka key;
        private SoftValue(Ka aKey, Va aValue, ReferenceQueue<Va> q) 
        {
            super(aValue, q);
            this.key = aKey;
        }
    }
    
    private class E implements Map.Entry<K, SoftValue<K,V>>
    {
    	private K k;
    	private SoftValue<K,V> v;
    	public E(K k, V v) { this.k = k; this.v = new SoftValue<K,V>(k, v, queue); }
    	public SoftValue<K,V> getValue() { return v; }
    	public SoftValue<K,V> setValue(SoftValue<K,V> v) { SoftValue<K,V> old = v; this.v = v; return old;}
    	public K getKey() { return k; }
    }
    
    private static class F<K, V> implements Map.Entry<K, V>
    {
    	private K k;
    	private V v;
    	public F(K k, V v) { this.k = k; this.v = v; }
    	public V getValue() { return v; }
    	public V setValue(V v) { V old = v; this.v = v; return old;}
    	public K getKey() { return k; } 
    }
 
    private final Map<K, SoftValue<K, V>> hash;
    private final ReferenceQueue<V> queue = new ReferenceQueue<V>();
    
    private void processQueue() 
    {
        SoftValue<K, V> sv;
        while ((sv = (SoftValue<K,V>) queue.poll()) != null) 
        {
            hash.remove(sv.key);
        }
    }
    
    public SoftHashMap() 
    {
        hash = new HashMap<K, SoftValue<K, V>>();
    }

    public SoftHashMap(int initialCapacity)
    {
    	hash = new HashMap<K, SoftValue<K, V>>(initialCapacity);
    }
    
    public SoftHashMap(int initialCapacity, float loadFactor)
    {
    	hash = new HashMap<K, SoftValue<K, V>>(initialCapacity, loadFactor);    	
    }
    
    public SoftHashMap(Map<? extends K,? extends V> m)
    {
    	this();
    	for (Map.Entry<? extends K, ? extends V> e : m.entrySet())
    	{ 
    		hash.put(e.getKey(), new SoftValue<K,V>(e.getKey(), e.getValue(), queue));
    	}
    }
    
    @Override
    public V get(Object key) 
    {
        V result = null;
        SoftValue<K, V> soft_ref = hash.get(key);
        if (soft_ref != null) 
        {
            result = soft_ref.get();
            if (result == null) 
                hash.remove(key);
        }
        return result;
    }

    public V put(K key, V value) 
    {
        processQueue();
        hash.put(key, new SoftValue<K,V>(key, value, queue));
        return value;
    }

    public V remove(Object key) 
    {
        processQueue();
        SoftValue<K,V> soft = hash.remove(key);
        return soft == null ? null : soft.get();
    }

    public void clear() 
    {
        processQueue();
        hash.clear();
    }

    public int size() 
    {
        processQueue();
        return hash.size();
    }

    public Set<Map.Entry<K, V>> entrySet() 
    {
    	final Set<Map.Entry<K, SoftValue<K,V>>> s = hash.entrySet();
    	return new Set<Map.Entry<K, V>>()
    	{
    		public boolean add(Map.Entry<K, V> e) 
    		{
    			return s.add(new E(e.getKey(), e.getValue()));
    		}

    		public boolean addAll(Collection<? extends Map.Entry<K, V>> c) 
    		{
    			boolean result = false;
    			for (Map.Entry<K,V> e : c)
    				if (this.add(e)) result = true;
    			return result;
    		}

    		public void clear() 
    		{
    			s.clear();
    		}

    		public boolean contains(Object o) 
    		{
    			if (! (o instanceof Map.Entry))
    				return false;
    			else
    			{
    				Map.Entry<K,V> e = (Map.Entry<K,V>)o;
    				return s.contains(new E(e.getKey(), e.getValue()));
    			}
    		}
    			
    		public boolean containsAll(Collection<?> c) 
    		{
    			for (Object x:c)
    				if (!this.contains(x)) 
    					return false;
    			return true;
    		}

    		public boolean isEmpty() 
    		{
    			return s.isEmpty();
    		}

    		public Iterator<Map.Entry<K, V>> iterator() 
    		{
    			final Iterator<Map.Entry<K, SoftValue<K, V>>> i = s.iterator();
    			return new Iterator<Map.Entry<K, V>>()
    			{
    				public void remove() { i.remove(); }
    				public boolean hasNext() { return i.hasNext(); }
    				public Map.Entry<K, V> next() 
    				{ 
    					Map.Entry<K, SoftValue<K, V>> e = i.next();
    					return new F<K,V>(e.getKey(), e.getValue().get());  
    				}
    			};
    		}

    		public boolean remove(Object o) 
    		{
    			if (! (o instanceof Map.Entry))
    				return false;
    			else
    			{
    				Map.Entry<K,V> e = (Map.Entry<K,V>)o;
    				return s.remove(new E(e.getKey(), e.getValue()));
    			}    		
    		}
    		public boolean removeAll(Collection c) 
    		{
    			boolean result = false;
    			for (Object x:c)
    				if (this.remove(x)) 
    					result = true;
    			return result;
    		}

    		public boolean retainAll(Collection c) 
    		{
    			throw new UnsupportedOperationException();
    		}

    		public int size() 
    		{
    			return s.size();
    		}

    		public Object[] toArray() 
    		{
    			return toArray(null);
    		}

    		public Object[] toArray(Object[] a) 
    		{ 
    			F [] result = null;
    			if (a != null && a instanceof F[] && a.length >= size())
    				result = (F[])a;
    			else
    				result = new F[size()];
    			Object [] A = s.toArray();
    			for (int i = 0; i < A.length; i++)
    			{
    				Map.Entry<K, SoftValue<K,V>> e = (Map.Entry<K, SoftValue<K,V>>)A[i];    				
    				result[i] = new F<K,V>(e.getKey(), e.getValue().get());
    			}
    			return result;

    		}    		
    	};
    }
}
