package org.hypergraphdb.transaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * <p>
 * Transactional linked list - random access is O(n)
 * </p>
 * 
 * @author Borislav Iordanov
 * 
 * @param <E>
 */
public class TxList<E> implements List<E>
{

    public boolean add(E e)
    {
        return false;
    }

    public void add(int index, E element)
    {
    }

    public boolean addAll(Collection<? extends E> c)
    {
        return false;
    }

    public boolean addAll(int index, Collection<? extends E> c)
    {
        return false;
    }

    public void clear()
    {
    }

    public boolean contains(Object o)
    {
        return false;
    }

    public boolean containsAll(Collection<?> c)
    {
        return false;
    }

    public E get(int index)
    {
        return null;
    }

    public int indexOf(Object o)
    {
        return 0;
    }

    public boolean isEmpty()
    {
        return false;
    }

    public Iterator<E> iterator()
    {
        return null;
    }

    public int lastIndexOf(Object o)
    {
        return 0;
    }

    public ListIterator<E> listIterator()
    {
        return null;
    }

    public ListIterator<E> listIterator(int index)
    {
        return null;
    }

    public E remove(int index)
    {
        return null;
    }

    public boolean remove(Object o)
    {
        return false;
    }

    public boolean removeAll(Collection<?> c)
    {
        return false;
    }

    public boolean retainAll(Collection<?> c)
    {
        return false;
    }

    public E set(int index, E element)
    {
        return null;
    }

    public int size()
    {
        return 0;
    }

    public List<E> subList(int fromIndex, int toIndex)
    {
        return null;
    }

    public Object[] toArray()
    {
        return null;
    }

    public <T> T[] toArray(T[] a)
    {
        return null;
    }
}