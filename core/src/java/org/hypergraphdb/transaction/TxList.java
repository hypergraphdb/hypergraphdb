/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

import java.lang.reflect.Array;
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
	private static class Node<E>
	{
		E value;
		VBox<Node<E>> next = null;
		public Node(E value, VBox<Node<E>> next)
		{
			this.value = value;
			this.next = next;
		}
	}
	
	HGTransactionManager txManager = null;
	private VBox<Integer> sizebox = null;
	private VBox<Node<E>> head = null;
	private VBox<Node<E>> tail = null;
	
	VBox<Node<E>> findNode(int index)
	{
		if (index < 0 || index >= sizebox.get())
			throw new IndexOutOfBoundsException("In TxList: " + index);
		VBox<Node<E>> result = head;
		while (index-- > 0)
			result = result.get().next;
		return result;
	}
	
	VBox<Node<E>> fromInitial(Iterator<E> I)
	{
		if (!I.hasNext())
			return new VBox<Node<E>>(txManager, null);
		sizebox = new VBox<Integer>(txManager, sizebox.get() + 1);		
		E currentElement = I.next();
		VBox<Node<E>> nextNodeBox = I.hasNext() ? fromInitial(I) : new VBox<Node<E>>(txManager, null);
		Node<E> currentNode = new Node<E>(currentElement, nextNodeBox); 
		if (!I.hasNext())
			tail = new VBox<Node<E>>(txManager, currentNode);
		return new VBox<Node<E>>(txManager, currentNode);
	}
	
	public TxList(HGTransactionManager txManager)
	{
		this.txManager = txManager;
		sizebox = new VBox<Integer>(txManager, 0);
		tail = new VBox<Node<E>>(txManager, null);		
		head = new VBox<Node<E>>(txManager, null);
	}

	public TxList(HGTransactionManager txManager, Iterable<E> initialData)
	{
		this(txManager);		
		head = fromInitial(initialData.iterator());
	}
	
    public boolean add(E e)
    {
    	Node<E> node = new Node<E>(e, new VBox<Node<E>>(txManager, null));    	
    	if (isEmpty())
    		head.put(node);
    	else
    	{
    		VBox<Node<E>> lastBox = head;
    		while(lastBox.get() != null)
    			lastBox = lastBox.get().next;
    		lastBox.put(node);
    	}
		tail.put(node);
		sizebox.put(sizebox.get() + 1);
        return true;
    }

    public void add(int index, E e)
    {
    	Node<E> node = new Node<E>(e, new VBox<Node<E>>(txManager, null));
    	if (index == 0) // this should become the first element
    	{
    		if (isEmpty()) // if the list is currently empty
    			tail.put(node);    		
    		node.next.put(head.get());
    		head.put(node);
    	}
    	else
    	{
	    	// find the node after which we need to insert
	    	Node<E> prev = findNode(index - 1).get(); 
	   		node.next.put(prev.next.get());
	   		prev.next.put(node);
    	}
    	sizebox.put(sizebox.get() + 1);
    }

    public boolean addAll(Collection<? extends E> c)
    {
    	for (E x : c)
    		add(x);
    	return true;
    }

    public boolean addAll(int index, Collection<? extends E> c)
    {
    	Node<E> prev = null;
    	if (index > 0)
    		prev = findNode(index - 1).get();
    	for (E e : c)
    	{
    		Node<E> node = new Node<E>(e, new VBox<Node<E>>(txManager, null));
    		if (prev == null)
    		{
    			node.next.put(head.get());
    			head.put(node);
    		}
    		else
    		{
    			node.next.put(prev.next.get());
    			prev.next.put(node);
    		}
    		if (tail.get() == prev) // if we're inserting at the end of the list, move the tail
    			tail.put(node);    		
    		prev = node;    		
    	}
    	sizebox.put(sizebox.get() + c.size());
        return true;
    }

    public void clear()
    {
    	head.put(null);
    	tail.put(null);
    	sizebox.put(0);
    }

    public boolean contains(Object o)
    {
        return indexOf(o) >= 0;
    }

    public boolean containsAll(Collection<?> c)
    {
        for (Object x : c)
        	if (!contains(x))
        		return false;
        return true;
    }

    public E get(int index)
    {
        Node<E> x = findNode(index).get();
        return x.value;
    }

    public int indexOf(Object o)
    {
    	int i = 0;
        for (Node<E> current = head.get(); current != null; current = current.next.get())
        {	
        	if (current.value == o || o != null && o.equals(current.value)) 
        		return i;
        	i++;
        }
        return -1;
    }

    public boolean isEmpty()
    {
        return sizebox.get() == 0;
    }

    public Iterator<E> iterator()
    {
        return new Iterator<E>()
        {
        	private Node<E> next = head.get();
        	
			public boolean hasNext()
			{
				return next != null;
			}

			public E next()
			{
				E x = next.value;
				if (next.next != null)
					next = next.next.get();
				else
					next = null;
				return x;
			}

			public void remove()
			{
				throw new UnsupportedOperationException();
			}        
        };
    }

    public int lastIndexOf(Object o)
    {
    	int last = -1;
    	int curr = 0;
        for (Node<E> current = head.get(); current != null; current = current.next.get())
        {	
        	if (current.value == o || o != null && o.equals(current.value)) 
        		last = curr;
        	curr++;
        }    	
        return last;
    }

    public ListIterator<E> listIterator()
    {
        throw new UnsupportedOperationException();
    }

    public ListIterator<E> listIterator(int index)
    {
    	throw new UnsupportedOperationException();
    }

    public E remove(int index)
    {
    	E old;
    	if (index < 0 || index >= size())
    		throw new IndexOutOfBoundsException("In TxList: " + index);
    	if (index == 0)
    	{
    		old = head.get().value;
    		head.put(head.get().next.get());
    		if (sizebox.get() == 1)
    			tail.put(null);
    	}
    	else
    	{
    		Node<E> prev = findNode(index - 1).get();
    		if (tail.get() == prev.next.get())
    			tail.put(prev);
    		old = prev.next.get().value;
    		// prev.next = prev.next.next:
    		prev.next.put(prev.next.get().next.get());    		
    	}
    	sizebox.put(sizebox.get() - 1);
        return old;
    }

    public boolean remove(Object o)
    {
    	boolean found = false;
    	Node<E> prev = null;
    	for (Node<E> current = head.get(); current != null; current = current.next.get())
    	{
    		if (current.value != o && !(o != null && o.equals(current.value)))
    		{
    			prev = current;
    			continue;
    		}
    		found = true;
    		if (tail.get() == current)
    			tail.put(prev);
    		if (prev == null) // removing the head
    			head.put(head.get().next.get());
    		else
    			// prev.next = current.next:
    			prev.next.put(current.next.get());
    	}
    	sizebox.put(sizebox.get() - 1);
        return found;
    }

    @SuppressWarnings("unchecked")
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
    	throw new UnsupportedOperationException();
    }

    public E set(int index, E element)
    {
        VBox<Node<E>> nodeBox = findNode(index);
        E old = nodeBox.get().value;
        nodeBox.put(new Node<E>(element, nodeBox.get().next));
        return old;
    }

    public int size()
    {
        return sizebox.get();
    }

    public List<E> subList(int fromIndex, int toIndex)
    {
    	throw new UnsupportedOperationException();
    }

    public Object[] toArray()
    {
        Object[] A = new Object[size()];
        int i = 0;
        for (Object x : this)
        	A[i++] = x;
        return A;
    }

    @SuppressWarnings("unchecked")
	public <T> T[] toArray(T[] a)
    {
    	Class<? extends T[]> type = (Class<? extends T[]>)a.getClass(); 
        T[] copy = ((Object)type == (Object)Object[].class)
        ? (T[]) new Object[size()]
        : (T[]) Array.newInstance(type.getComponentType(), size());
        int i = 0;
        for (T x : (Iterable<T>)this)
        	copy[i++] = x;
        return copy;
    }
}