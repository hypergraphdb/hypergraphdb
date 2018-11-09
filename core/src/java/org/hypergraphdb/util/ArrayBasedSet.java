/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.HGRandomAccessResult;

/**
 * 
 * <p>
 * An implementation <code>SortedSet</code> based on primitive arrays that grow
 * as needed without ever shrinking. Lookup is log(n), but insertion and removal
 * take longer obviously so this implementation is mainly suitable for small sets
 * of a few dozen elements, or for sets that are searched/iterated much more 
 * frequently than they are changed. The main reason for being of this implementation
 * is space efficiency: a red-black tree holds additional 12 bytes per datum. So while
 * for a large set, a tree should be used, the array-based implementation is preferable 
 * for many small sets like many of HyperGraphDB's incidence sets. 
 * </p>
 *
 * <p>
 * Some benchmarking experiments comparing this (rather simple) implementation to red-black
 * trees (both LLRBTree and the standard Java TreeSet): working with about up to 10000 elements,
 * insertion and removal have a comparable performance, with the array-based implementation
 * being about 15% slower (elements inserted/removed in random order). The LLRBTree implementation
 * is actually noticeably slower than TreeSet, probably due to recursion. However, in "read-mode", 
 * when iterating over the set, using it as a HGRandomAccessResult, the array-based implementation
 * is 8-10 faster. Understandable since here moving to the next element amounts to incrementing an integer
 * while in a tree a lookup for the successor must be performed (e.g. min(parent(current))). So for
 * set of this order of magnitude and/or sets that are being read more than they are modified, 
 * it is strongly advisable to use the ArrayBasedSet. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <E>
 */
@SuppressWarnings("unchecked")
public class ArrayBasedSet<E> implements HGSortedSet<E>, CloneMe
{
	Class<E> type;
	E [] array;
	Comparator<E> comparator = null;
	int size = 0;
	private ReadWriteLock lock = new ReentrantReadWriteLock();
	
	int lookup(E key)
	{
		int low = 0;
		int high = size-1;

		Comparable<E> ckey = comparator == null ? (Comparable<E>)key : null;
		while (low <= high) 
		{
			int mid = (low + high) >> 1;
	    	E midVal = array[mid];
	    	int cmp = ckey == null ? comparator.compare(midVal, key) : -ckey.compareTo(midVal);
	    	if (cmp < 0)
	    		low = mid + 1;
	    	else if (cmp > 0)
	    		high = mid - 1;
	    	else
	    		return mid; // key found
		}
		return -(low + 1);  // key not found.		
	}
			
	/**
	 * <p>
	 * Initialize from the given array.
	 * </p>
	 * 
	 * @param A The array used to initialize. An internal array is created 
	 * with the same size as <code>A</code> and all its elements are copied.
	 */
	public ArrayBasedSet(E [] A)
	{
		this(A, null);
	}

	/**
	 * <p>
	 * Initialize an empty set with a given initial capacity.
	 * </p>
	 * 
	 * @param A The array used to initialize. An internal array is created 
	 * with the same type as as <code>A</code> and with <code>size</code> number
	 * of slots. 
	 */
	public ArrayBasedSet(E [] A, int capacity)
	{
		this(A, capacity, null);
	}
	
	/**
	 * <p>
	 * Initialize an empty set with a given initial capacity, and a given 
	 * comparator.
	 * </p>
	 * 
	 * @param A The array used to initialize. An internal array is created 
	 * with the same type as as <code>A</code> and with <code>size</code> number
	 * of slots. 
	 * @param Comparator The comparator used to compare elements. 
	 */
	public ArrayBasedSet(E [] A, int capacity, Comparator<E> comparator)
	{
		type = (Class<E>)A.getClass().getComponentType();
		array = (E[])java.lang.reflect.Array.newInstance(type, capacity);
		size = 0;
		this.comparator = comparator;
	}
	
	/**
	 * <p>
	 * Initialize from the given array and with the given <code>Comparator</code>.
	 * </p>
	 * 
	 * @param A The array used to initialize. An internal array is created 
	 * with the same size as <code>A</code> and all its elements are copied.
	 * @param Comparator The comparator used to compare elements.
	 */	
	public ArrayBasedSet(E [] A, Comparator<E> comparator)
	{
		this.comparator = comparator;		
		type = (Class<E>)A.getClass().getComponentType();
		array = (E[])java.lang.reflect.Array.newInstance(type, A.length);
		System.arraycopy(A, 0, array, 0, A.length);	
		size = A.length;
	}
	
	public void setFromArray(E [] A)
	{
		lock.writeLock().lock();
		try
		{
			if (array.length < A.length)
				array = (E[])java.lang.reflect.Array.newInstance(type, A.length);
			System.arraycopy(A, 0, array, 0, A.length);
			this.size = A.length;
		}
		finally
		{
			lock.writeLock().unlock();
		}		
	}
		
	public ReadWriteLock getLock()
	{
		return lock;
	}

	public void setLock(ReadWriteLock lock)
	{
		if (lock == null)
			throw new NullPointerException("ArrayBasedSet.lock can't be null.");
		this.lock = lock;
	}

	public Comparator<? super E> comparator()
	{
		return comparator;
	}

	public E getAt(int i)
	{
		lock.readLock().lock();
		try
		{
			if (i < 0 || i > size())
				throw new IllegalArgumentException("index " + i + " out of bounds [0," + size() + ").");
			else
				return array[i];
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public E removeAt(int i)
	{
		lock.writeLock().lock();
		try
		{
			if (i < 0 || i > size())
				throw new IllegalArgumentException("index " + i + " out of bounds [0," + size() + ").");
			else
			{
				E result = array[i];
				if (i < size - 1) // if it's not the last element			
					System.arraycopy(array, i + 1, array, i, size - i - 1);
				size--;				
				return result;
			}
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}
	
	public E first()
	{
		lock.readLock().lock();
		try
		{
			if (size == 0)
				throw new NoSuchElementException();
			return array[0];
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public SortedSet<E> headSet(E toElement)
	{
		throw new UnsupportedOperationException("...because of lazy implementor: this is a TODO.");
	}

	public E last()
	{
		lock.readLock().lock();
		try
		{
			if (size == 0)
				throw new NoSuchElementException();
			return array[size-1];
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public SortedSet<E> subSet(E fromElement, E toElement)
	{
		throw new UnsupportedOperationException("...because of lazy implementor: this is a TODO.");
	}

	public SortedSet<E> tailSet(E fromElement)
	{
		throw new UnsupportedOperationException("...because of lazy implementor: this is a TODO.");
	}

	public boolean add(E o)
	{
		lock.writeLock().lock();
		try
		{
			int idx = lookup((E)o);
			
			if (idx >= 0)
				return false;
			else 
				idx = -(idx + 1);
			if (size < array.length)
			{
				System.arraycopy(array, idx, array, idx + 1, size - idx);
				array[idx] = o;
			}
			else // need to grow the array...
			{
				E [] tmp = (E[])java.lang.reflect.Array.newInstance(type, (int)(1.5 * size) + 1);
				System.arraycopy(array, 0, tmp, 0, idx);
				tmp[idx] = o;
				System.arraycopy(array, idx, tmp, idx + 1, size - idx);
				array = tmp;
			}
			size++;
			return true;
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public boolean addAll(Collection<? extends E> c)
	{
		boolean modified = false;
		for (Object x : c)
			if (add((E)x))
				modified = true;
		return modified;
	}

	public void clear()
	{
		lock.writeLock().lock();
		size = 0;
		lock.writeLock().unlock();
	}

	public boolean contains(Object o)
	{
		lock.readLock().lock();
		try { return lookup((E)o) >= 0; }
		finally { lock.readLock().unlock(); }
	}

	public boolean containsAll(Collection<?> c)
	{
		for (Object x  : c)
			if (!contains(x))
				return false;
		return true;
	}

	public boolean isEmpty()
	{
		lock.readLock().lock();
		try { return size == 0; }
		finally { lock.readLock().unlock(); }
	}

	public Iterator<E> iterator()
	{
		return new ResultSet(false);
	}

	public HGRandomAccessResult<E> getSearchResult()
	{
		return new ResultSet(true);
	}
	
	public boolean remove(Object o)
	{
		lock.writeLock().lock();
		try
		{
			int idx = lookup((E)o);
			if (idx < 0)
				return false;
			else if (idx < size - 1) // if it's not the last element			
				System.arraycopy(array, idx + 1, array, idx, size - idx - 1);
			size--;
			return true;
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public boolean removeAll(Collection<?> c)
	{
		boolean modified = false;
		for (Object x : c)
			if (remove((E)x))
				modified = true;
		return modified;
	}

	public boolean retainAll(Collection<?> c)
	{
		lock.writeLock().lock();
		try
		{
			boolean modified = false;
			for (int i = 0; i < size; i++)
				if (!c.contains(array[i]))
				{
					System.arraycopy(array, i + 1, array, i, size - i);
					size--;
					modified = true;
				}
			return modified;
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}

	public int size()
	{
		lock.readLock().lock();
		try { return size; }
		finally { lock.readLock().unlock(); }
	}

	public Object[] toArray()
	{
		lock.readLock().lock();
		try
		{
			int len = size();
			Object [] A = new Object[len];
			System.arraycopy(array, 0, A, 0, len);
			return A;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public ArrayBasedSet<E> duplicate()
	{
	    try
	    {
    	    ArrayBasedSet<E> S = (ArrayBasedSet<E>)super.clone();    	    
            S.array = ((Object)array.getClass() == (Object)Object[].class)
                ? (E[]) new Object[size]
                : (E[]) Array.newInstance(array.getClass().getComponentType(), size);
            System.arraycopy(array, 0, S.array, 0, size);
    	    return S;
	    } 
	    catch (CloneNotSupportedException e) 
	    {
	        // this shouldn't happen, since we are Cloneable
	        throw new InternalError();
	    }    	    
	}
	
	public <T> T[] toArray(T[] a)
	{
	    lock.readLock().lock();
	    try
	    {
            if (a.length < size)
            {
                Class<? extends T[]> type = (Class<? extends T[]>)a.getClass(); 
                T[] copy = ((Object)type == (Object)Object[].class)
                ? (T[]) new Object[size]
                : (T[]) Array.newInstance(type.getComponentType(), size);
                System.arraycopy(array, 0, copy, 0,
                                 Math.min(array.length, size));
                return copy;                
            }
            System.arraycopy(array, 0, a, 0, size);
            if (a.length > size)
                a[size] = null;
            return a;
	    }
	    finally
	    {
	        lock.readLock().unlock();
	    }
	}
	
	class ResultSet implements HGRandomAccessResult<E>
	{
		int pos = -1;
		boolean locked;
		
		ResultSet(boolean locked) 
		{ 
			this.locked = locked;
			if (locked)
				lock.readLock().lock();
		}
		public GotoResult goTo(E value, boolean exactMatch)
		{
			int idx = lookup(value);			
			if (idx >= 0)
			{
				pos = idx;
				return GotoResult.found;
			}
			else if (exactMatch)
				return GotoResult.nothing;
			else
			{
				idx = -(idx + 1);
				if (idx >= size)
					return GotoResult.nothing;
				pos = idx;
				return GotoResult.close;
			}
		}

		public void goBeforeFirst()
		{
		    pos = -1;
		}
		
		public void goAfterLast()
		{
		    pos = size;
		}
		
		public boolean hasPrev()
		{
			return pos > 0;
		}

		public E prev()
		{
			return array[--pos];
		}

		public boolean hasNext()
		{
			return pos + 1 < size;
		}

		public E next()
		{
			return array[++pos];
		}

		public void remove()
		{
			throw new UnsupportedOperationException("...because of lazy implementor: this is a TODO.");
		}

		public void close()
		{
			if (locked)
			{
				lock.readLock().unlock();
				locked = false;
			}
		}

		public E current()
		{
			if (pos < 0 || pos >= size)
				throw new NoSuchElementException();
			return array[pos];
		}

		public boolean isOrdered()
		{
			return true;
		}		
	}
}