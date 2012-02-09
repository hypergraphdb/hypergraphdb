package org.hypergraphdb.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.query.impl.FilteredRAResultSet;

public class FilteredSortedSet<E> implements HGSortedSet<E>
{
    private HGSortedSet<E> delegate;
    private Mapping<E, Boolean> predicate;
    
    public FilteredSortedSet(HGSortedSet<E> delegate, Mapping<E, Boolean> predicate)
    {
        this.delegate = delegate;
        this.predicate = predicate;
    }
    
    public boolean add(E e)
    {
        if (!predicate.eval(e))
            return false;
        return delegate.add(e);
    }

    public boolean addAll(Collection<? extends E> c)
    {
        for (E e : c)
            if (!predicate.eval(e))
                return false;
        return delegate.addAll(c);
    }

    public void clear()
    {
        for (E e : this)
            if (predicate.eval(e))
                delegate.remove(e);
//        delegate.clear();
    }

    public Comparator<? super E> comparator()
    {
        return delegate.comparator();
    }

    @SuppressWarnings("unchecked")
    public boolean contains(Object o)
    {
        return predicate.eval((E)o) && delegate.contains(o);
    }

    public boolean containsAll(Collection<?> c)
    {
        for (E e : this)
            if (!predicate.eval(e) || !!delegate.contains(e))
                return false;
        return true;
    }

    public boolean equals(Object o)
    {
        int matches = 0;
        @SuppressWarnings("unchecked")
        HGSortedSet<E> S = (HGSortedSet<E>)o;
        for (E e : S)
            if (contains(e))
                matches++;
        return matches == S.size(); 
    }

    public E first()
    {
        HGRandomAccessResult<E> rs = delegate.getSearchResult();
        try
        {
            while (rs.hasNext())
                if (predicate.eval(rs.next()))
                    return rs.current();
        }
        finally
        {
            rs.close();
        }
        throw new NoSuchElementException();
    }

    public HGRandomAccessResult<E> getSearchResult()
    {
        return new FilteredRAResultSet<E>(delegate.getSearchResult(), predicate, 0);
    }

    public int hashCode()
    {
        return delegate.hashCode();
    }

    public SortedSet<E> headSet(E toElement)
    {
        return delegate.headSet(toElement);
    }

    public boolean isEmpty()
    {
        //2012.02.09 hilpold old: return size() == 0;
    	// we can conclude false more efficient by looking for a first.
        HGRandomAccessResult<E> rs = delegate.getSearchResult();
        try
        {
            while (rs.hasNext())
                if (predicate.eval(rs.next()))
                    return false;
        }
        finally
        {
            rs.close();
        }
        return true;
    }

    public Iterator<E> iterator()
    {
        return new FilterIterator<E>(delegate.iterator(), predicate);
    }

    public E last()
    {
        HGRandomAccessResult<E> rs = delegate.getSearchResult();
        rs.goAfterLast();
        try
        {
            while (rs.hasPrev())
                if (predicate.eval(rs.prev()))
                    return rs.current();
        }
        finally
        {
            rs.close();
        }
        throw new NoSuchElementException();
    }

    @SuppressWarnings("unchecked")
    public boolean remove(Object o)
    {
        return predicate.eval((E)o) && delegate.remove(o);
    }

    @SuppressWarnings("unchecked")
    public boolean removeAll(Collection<?> c)
    {
        boolean changed = false;
        for (E e : (Collection<E>)c)
            if (predicate.eval(e))
                changed = changed || delegate.remove(e);
        return changed;
    }

    public boolean retainAll(Collection<?> c)
    {
        throw new UnsupportedOperationException();
    }

    public int size()
    {
        return toArray().length;
//        int cnt = 0;
//        HGRandomAccessResult<E> rs = delegate.getSearchResult();
//        try
//        {
//            while (rs.hasNext())
//                if (predicate.eval(rs.next()))
//                   cnt++;
//        }
//        finally
//        {
//            rs.close();
//        }
//        return cnt;
    }

    public SortedSet<E> subSet(E fromElement, E toElement)
    {
        return delegate.subSet(fromElement, toElement);
    }

    public SortedSet<E> tailSet(E fromElement)
    {
        return delegate.tailSet(fromElement);
    }

    public Object[] toArray()
    {
        ArrayList<Object> L = new ArrayList<Object>();
        HGRandomAccessResult<E> rs = delegate.getSearchResult();
        try
        {
            while (rs.hasNext())
                if (predicate.eval(rs.next()))
                    L.add(rs.current());
        }
        finally
        {
            rs.close();
        }
        return L.toArray();
    }

    public <T> T[] toArray(T[] a)
    {
        ArrayList<Object> L = new ArrayList<Object>();
        HGRandomAccessResult<E> rs = delegate.getSearchResult();
        try
        {
            while (rs.hasNext())
                if (predicate.eval(rs.next()))
                    L.add(rs.current());
        }
        finally
        {
            rs.close();
        }
        return L.toArray(a);
    }
}