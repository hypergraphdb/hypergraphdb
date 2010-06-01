package org.hypergraphdb.storage;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;
import org.hypergraphdb.util.HGSortedSet;

public class StorageBasedIncidenceSet implements HGSortedSet<HGHandle>
{
    private HGHandle atom;
    private HyperGraph graph;

    public StorageBasedIncidenceSet(HGHandle atom, HyperGraph graph)
    {
        this.atom = atom;
        this.graph = graph;
    }

    @SuppressWarnings("unchecked")
    public HGRandomAccessResult<HGHandle> getSearchResult()
    {
        return (HGRandomAccessResult<HGHandle>)(HGRandomAccessResult<?>)
            graph.getStore().getIncidenceResultSet(graph.getPersistentHandle(atom));
    }

    @SuppressWarnings("unchecked")
    public Comparator<? super HGHandle> comparator()
    {
        return new Comparator()
        {
            public int compare(Object x, Object y)
            {
                return ((Comparable)x).compareTo(y);
            }
        };
    }

    public HGHandle first()
    {
        HGRandomAccessResult<HGHandle> rs = getSearchResult();
        try
        {
            if (!rs.hasNext())
                throw new NoSuchElementException();
            return rs.next();
        }
        finally
        {
            rs.close();
        }
    }

    public SortedSet<HGHandle> headSet(HGHandle toElement)
    {
        throw new UnsupportedOperationException();
    }

    public HGHandle last()
    {
        HGRandomAccessResult<HGHandle> rs = getSearchResult();
        try
        {
            rs.goAfterLast();            
            if (!rs.hasPrev())
                throw new NoSuchElementException();
            return rs.prev();
        }
        finally
        {
            rs.close();
        }
    }

    public SortedSet<HGHandle> subSet(HGHandle fromElement, HGHandle toElement)
    {
        throw new UnsupportedOperationException();
    }

    public SortedSet<HGHandle> tailSet(HGHandle fromElement)
    {
        throw new UnsupportedOperationException();
    }

    public boolean add(HGHandle e)
    {
        if (contains((HGHandle)e))
            return false;
        graph.getStore().addIncidenceLink(graph.getPersistentHandle(atom), graph.getPersistentHandle(e));
        return true;        
    }

    public boolean addAll(Collection<? extends HGHandle> c)
    {
        boolean modified = false;
        for (HGHandle x : c)
            modified = modified || add(x);
        return modified;
    }

    public void clear()
    {
        graph.getStore().removeIncidenceSet(graph.getPersistentHandle(atom));
    }

    public boolean contains(Object o)
    {
        HGRandomAccessResult<HGHandle> rs = getSearchResult();
        try
        {
            return rs.goTo((HGHandle)o, true) == GotoResult.found;
        }
        finally
        {
            rs.close();
        }
    }

    @SuppressWarnings("unchecked")
    public boolean containsAll(Collection<?> c)
    {
        HGRandomAccessResult<HGHandle> rs = getSearchResult();
        try 
        { 
            for (HGHandle x : (Collection<HGHandle>)c)            
                if (rs.goTo(x, true) != HGRandomAccessResult.GotoResult.found)
                    return false;
            return true;
        }
        finally
        {
            rs.close();
        }        
    }

    public boolean isEmpty()
    {
        HGRandomAccessResult<HGHandle> rs = getSearchResult();
        try
        {
            return !rs.hasNext();
        }
        finally
        {
            rs.close();
        }
    }

    public Iterator<HGHandle> iterator()
    {
        throw new UnsupportedOperationException("Use getSearchResult and make sure you close it.");
    }

    public boolean remove(Object o)
    {
        if (contains(o))
        {
            graph.getStore().removeIncidenceLink(graph.getPersistentHandle(atom), 
                        graph.getPersistentHandle((HGHandle)o));
            return true;
        }
        else
            return false;
    }

    public boolean removeAll(Collection<?> c)
    {
        boolean modified = false;
        for (Object x : c)
            modified = modified || remove(x);
        return modified;
    }

    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        return (int)graph.getStore().getIncidenceSetCardinality(graph.getPersistentHandle(atom));
    }

    public Object[] toArray()
    {
        HGRandomAccessResult<HGHandle> rs = getSearchResult();
        try
        {
            int size = size();
            Object [] a = new Object[size];
            for (int i = 0; i < size; i++)
                a[i] = rs.next();
            return a;
        }
        finally
        {
            rs.close();
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(T[] a)
    {
        HGRandomAccessResult<HGHandle> rs = getSearchResult();
        try
        {
            int size = size();
            if (a.length < size)
                a = (T[])java.lang.reflect.Array
            .newInstance(a.getClass().getComponentType(), size);
            for (int i = 0; i < size; i++)
                a[i] = (T)rs.next();
            if (a.length > size)
                a[size] = null;
            return a;
        }
        finally
        {
            rs.close();
        }    }
}