package org.hypergraphdb.transaction;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hypergraphdb.util.RefResolver;

/**
 * 
 * <p>
 * A transactional map - every operation on this map is conducted within a transaction and
 * becomes void if the transaction is aborted. This map doesn't support <code>null</code>s 
 * as values - a null indicates absence of a value.
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <K>
 * @param <V>
 */
public class TxMap<K, V> implements Map<K, V>
{
    //private ConcurrentMap<K, VBox<V>> M = new ConcurrentHashMap<K, VBox<V>>();
    protected Map<K, Box> M = null;
    protected HGTransactionManager txManager;
    protected RefResolver<Object, Box> boxGetter = null;
    protected VBox<Integer> sizebox = null;
    
    protected class Box extends VBox<V>
    {
        WeakReference<K> key;
        
        public Box(HGTransactionManager txManager, K key)
        {
            super(txManager);
            this.key = new WeakReference<K>(key);
        }
        
        public VBoxBody<V> makeNewBody(V value, long version, VBoxBody<V> next)
        {
            return new BoxBody(value, version, next);
        }

        private class BoxBody extends VBoxBody<V>
        {            
            public BoxBody(V value, long version, VBoxBody<V> next)
            {
                super(value, version, next);
            }
            
            public void clearPrevious()
            {
                super.clearPrevious();
                if (value == null && body == this)
                {
                    synchronized (M)
                    {                       
                        M.remove(key.get());
                    }
                }
            }
        }
        
        public void finish(HGTransaction tx)
        {
            if (body.version == 0)
                synchronized (M) { M.remove(key.get()); }
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
            if (box == null)
            {
                box = new Box(txManager, (K)key);
                M.put((K)key, box);
            }
            return box;
            // Use this with a ConcurrentMap:
            // return (box == null) ? M.putIfAbsent((K)key, new VBox<V>(txManager.getContext())) : box;
        }
    }

    /**
     * <p>
     * The constructor expects a {@link HGTransactionManager} and the map to be used
     * to hold the entries. The <code>backingMap</code> parameter must be an empty map
     * and it is not fully typed since internal object wrappers will be used to manage
     * the transactional aspect.
     * </p>
     */
    @SuppressWarnings("unchecked")
    public TxMap(HGTransactionManager tManager, Map backingMap)
    {
        this.txManager = tManager;
        this.M = backingMap == null ? new ConcurrentHashMap<K, Box>(): backingMap;        
        this.sizebox = new VBox<Integer>(txManager, 0);
        if (this.M instanceof ConcurrentMap)            
            boxGetter = new RefResolver<Object,Box>() 
        {
            private ConcurrentMap<K, Box> cm = (ConcurrentMap<K, Box>)M;
            public Box resolve(Object k) 
            { 
                Box box = cm.get(k);
                if (box == null)
                    box = new Box(txManager, (K)k);
                Box box2 = cm.putIfAbsent((K)k, box);
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

    public int mapSize()
    {
        return M.size();
    }
    
    public int size()
    {
        return sizebox.get();
    }

    public Set<K> keySet()
    {
        return M.keySet();
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