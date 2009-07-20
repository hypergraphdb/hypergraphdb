package org.hypergraphdb.util;

import java.util.NoSuchElementException;

/**
 * <p>
 * A simple, non thread-safe stack, missing from java.util.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <T>
 */
public class SimpleStack<T>
{
    private static class Node<T>
    {
        T data;
        Node<T> next;
        public Node(T data, Node<T> next) { this.data = data; this.next = next; }
    }
    
    private int size = 0;
    private Node<T> top = null;
    
    public SimpleStack()
    {        
    }
    
    public void push(T data) 
    { 
        top = new Node<T>(data, top);
        size++;
    }
    
    public T peek() 
    { 
        if (top == null)
            throw new NoSuchElementException();
        else
            return top.data;
    }

    /**
     * <p>
     * Take a look at the element <code>depth</code> levels deep beneath
     * the top element (which is at depth 0). This requires O(depth) steps. 
     * </p>
     * 
     * @param depth
     * @return
     */
    public T peek(int depth) 
    { 
        if (depth >= size)
            throw new NoSuchElementException();
        else 
        {
            Node<T> curr = top;
            while (depth-- > 0) curr = curr.next;
            return curr.data;
        }
    }
    
    public T pop() 
    { 
        if (top == null)
            throw new NoSuchElementException();
        else
        {
            T x = top.data;
            top = top.next;
            size--;
            return x;
        }
    }
    
    public boolean isEmpty() { return top == null; }
    public int size() { return size; }
}