package org.hypergraphdb.storage;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.util.HGSortedSet;

/**
 * 
 * <p>
 * A database-backed <code>HGSortedSet</code> implementation representing the 
 * ordered duplicate values associated with a single key.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class DBKeyedSortedSet<T> implements HGSortedSet<T>
{
	
	public HGRandomAccessResult<T> getSearchResult()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Comparator<T> comparator()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public T first()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public SortedSet<T> headSet(T toElement)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public T last()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public SortedSet<T> subSet(T fromElement, T toElement)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public SortedSet<T> tailSet(T fromElement)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public boolean add(T o)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public boolean addAll(Collection c)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public void clear()
	{
		// TODO Auto-generated method stub

	}

	public boolean contains(Object o)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public boolean containsAll(Collection c)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public boolean isEmpty()
	{
		// TODO Auto-generated method stub
		return false;
	}

	public Iterator<T> iterator()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public boolean remove(Object o)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public boolean removeAll(Collection c)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public boolean retainAll(Collection c)
	{
		// TODO Auto-generated method stub
		return false;
	}

	public int size()
	{
		// TODO Auto-generated method stub
		return 0;
	}

	public Object[] toArray()
	{
		// TODO Auto-generated method stub
		return null;
	}

	public Object[] toArray(Object[] a)
	{
		// TODO Auto-generated method stub
		return null;
	}
}