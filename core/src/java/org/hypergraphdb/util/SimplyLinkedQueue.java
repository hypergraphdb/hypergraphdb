package org.hypergraphdb.util;

import java.util.NoSuchElementException;

public class SimplyLinkedQueue<T>
{
	private static class Node<T>
	{
		T data;
		Node<T> next;
		public Node(T data, Node<T> next) { this.data = data; this.next = next; }
	}
	
	private int size = 0;
	private Node<T> head;
	private Node<T> tail;
	
	public SimplyLinkedQueue()
	{
		
	}
	
	public void put(T data) 
	{ 
		Node<T> n = new Node<T>(data, null);
		if (tail != null)
			tail.next = n;
		else
			head = tail = n;
		size++;
	}
	
	public T fetch() 
	{ 
		if (head == null)
			throw new NoSuchElementException();
		T result = head.data;
		head = head.next;
		if (head == null)
			tail = null;
		size--;
		return result;
	}
	
	public T peekFront() 
	{ 
		if (head == null)
			throw new NoSuchElementException();
		else
			return head.data;
	}
	
	public T peekBack() 
	{ 
		if (tail == null)
			throw new NoSuchElementException();
		else
			return tail.data;		
	}
	
	public boolean isEmpty() { return tail == null; }
	public int size() { return size; }
}
