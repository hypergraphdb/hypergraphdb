/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

import org.hypergraphdb.HGIndex;
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
@SuppressWarnings("unchecked")
public class DBKeyedSortedSet<Key, T> implements HGSortedSet<T>
{
	private Key key;
	private Comparator<T> comparator = null;
	private HGIndex<Key, T> index = null;
	
	static <E> Comparator<E> makeComparator()
	{
		return new Comparator<E>()
		{
			public int compare(E x, E y)
			{
				return ((Comparable)x).compareTo(y);
			}
		};		
	}
	
	public DBKeyedSortedSet(HGIndex<Key, T> idx, Key key)
	{
		this.index = idx;
		this.key = key;
		comparator = makeComparator();
	}

	public DBKeyedSortedSet(HGIndex<Key, T> idx, Key key, Comparator<T> comparator)
	{
		this.index = idx;
		this.key = key;
		this.comparator = comparator;		 
	}
	
	
	public HGRandomAccessResult<T> getSearchResult()
	{
		return index.find(key);
	}

	public Comparator<T> comparator()
	{
		return comparator;
	}

	public T first()
	{
		return index.findFirst(key);
	}

	public SortedSet<T> headSet(T toElement)
	{
		throw new UnsupportedOperationException();
	}

	public T last()
	{
		throw new UnsupportedOperationException("No easy BerkeleyDB method for this one, need to iterate until the end - unefficient.");
	}

	public SortedSet<T> subSet(T fromElement, T toElement)
	{
		throw new UnsupportedOperationException();	}

	public SortedSet<T> tailSet(T fromElement)
	{
		throw new UnsupportedOperationException();	
	}

	public boolean add(T o)
	{
		if (contains(o))
			return false;
		index.addEntry(key, o);
		return true;
	}

	public boolean addAll(Collection c)
	{
		boolean modified = false;
		for (T x : (Collection<T>)c)
			modified = modified || add(x);
		return modified;
	}

	public void clear()
	{
		index.removeAllEntries(key);
	}

	public boolean contains(Object o)
	{
		HGRandomAccessResult<T> rs = getSearchResult();
		try 
		{ 
			return rs.goTo((T)o, true) == HGRandomAccessResult.GotoResult.found; 
		}
		finally
		{
			rs.close();
		}
	}

	public boolean containsAll(Collection c)
	{
		HGRandomAccessResult<T> rs = getSearchResult();
		try 
		{ 
			for (T x : (Collection<T>)c)			
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
		return first() == null;
	}

	/**
	 * <p>
	 * This iterator is intended for use when full iteration is performed on the set. 
	 * Otherwise, the underlying DB cursor remains open and locks DB pages forever.
	 * </p>
	 */
	public Iterator<T> iterator()
	{
		throw new UnsupportedOperationException("Use getSearchResult and make sure you close it.");
	}

	public boolean remove(Object o)
	{
		if (contains(o))
		{
			index.removeEntry(key, (T)o);
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
		return (int)index.count(key);
	}

	public Object[] toArray()
	{
		HGRandomAccessResult<T> rs = getSearchResult();
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

	public <E> E[] toArray(E[] a)
	{
		HGRandomAccessResult<T> rs = getSearchResult();
		try
		{
	        int size = size();
	        if (a.length < size)
	            a = (E[])java.lang.reflect.Array
			.newInstance(a.getClass().getComponentType(), size);
	        for (int i = 0; i < size; i++)
	        	a[i] = (E)rs.next();
	        if (a.length > size)
	        	a[size] = null;
	        return a;
		}
		finally
		{
			rs.close();
		}
	}
}
