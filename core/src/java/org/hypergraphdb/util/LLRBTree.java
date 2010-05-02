/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.AbstractSet;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;

/**
 * 
 * <p>
 * Implements a set of elements as a left-leaning red-black tree. The node
 * doesn't contain a pointer to a value, nor does it contains a pointer
 * to the parent which should make it more memory efficient than most
 * implementations (e.g. the standard java.util.TreeMap). However, tree
 * mutations are implemented recursively, which is not optimal and could
 * be removed in the future. Unfortunately, Java uses 4 bytes to store a boolean
 * so we don't gain as much in space compactness as we could theoretically, but
 * it's still an improvement. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <E> The type of elements this set stores. It must implement the <code>Comparable</code>
 * interface or a <code>Comparator</code> has to be provided at construction time.
 */
public class LLRBTree<E> extends AbstractSet<E>
						 implements HGSortedSet<E>, Cloneable, java.io.Serializable
{
	private static final long serialVersionUID = -1;
	
	private static final boolean RED = true;
	private static final boolean BLACK = false;

	// The node stack is used to keep track of parent pointers
	// during tree traversals. A fixed-size array is used for
	// the stack because it is known that it'll never go beyond
	// the depth of the tree which, since this a balanced tree,
	// is going to always be a relatively small number approx. 
	// equal to 2*log(treeSize).
	private final class NodeStack
	{
		Node<E> [] A;
		Node<E> [] B;
		int pos = -1;
		int bpos;
		
		@SuppressWarnings("unchecked")
		NodeStack()
		{
			int s = 0;
			if (size > 0)
				s = (int)(2*(Math.log(size + 1)/Math.log(2)));
			A = new Node[s];
			B = new Node[s];
		}
		
		void backup()
		{
			bpos = pos;
			System.arraycopy(A, 0, B, 0, pos+1);
		}
		void restore()
		{
			pos = bpos;
			Node<E> [] tmp = A; 
			A = B;
			B = tmp;
		}
		
		boolean isEmpty() { return pos < 0; }
		Node<E> top() { return A[pos]; }
		Node<E> pop() { return A[pos--]; }
		Node<E> push(Node<E> n) { return A[++pos] = n; } 
		void clear() { pos = -1; }
	}
	
	private static class Node<E> implements Cloneable
	{
		E key;
		Node<E> left, right;
		boolean color;
	
		@SuppressWarnings("unchecked")
		public Node<E> clone() throws CloneNotSupportedException
		{
			Node<E> n = (Node<E>)super.clone();
			if (left != null)
				n.left = left.clone();
			if (right != null)
				n.right = right.clone();
			return n;
		}
		
		Node(E key, boolean color)
		{
			this.key = key;			
			this.color = color;
		}
		
		Node<E> rotateLeft()
		{
			Node<E> x = right;
			right = x.left;
			x.left = this;
			x.color = this.color;
			this.color = RED;
			return x;
		}		
		
		Node<E> rotateRight()
		{
			Node<E> x = left;
			left = x.right;
			x.right = this;
			x.color = this.color;
			this.color = RED;
			return x;
		}
		
		Node<E> colorFlip()
		{
			color = !color;
			left.color = !left.color;
			right.color = !right.color;
			return this;
		}		
		
		private Node<E> fixUp()
		{
			Node<E> h = this;
			if (isRed(h.right))
			{
				h = h.rotateLeft();
				if (isRightLeaning(h.left))
					h.left = h.left.rotateLeft();
			}
			if (isRed(h.left) && isRed(h.left.left))
			{
				h = h.rotateRight();
			}
			if (isRed(h.left) && isRed(h.right))
			{
				h.colorFlip();
			}
			return h;
		}
		
		private Node<E> moveRedRight()
		{
			colorFlip();
			if (isRed(left.left))
			{
				Node<E> h = rotateRight();
				return h.colorFlip();
			}
			else
				return this;
		}		
		
		private Node<E> moveRedLeft()
		{
			colorFlip();
			if (isRed(right.left))
			{
				right = right.rotateRight();
				Node<E> h = rotateLeft();
				if (isRightLeaning(h.right))
					h.right = h.right.rotateLeft();
				return h.colorFlip();
			}
			else
				return this;
		}		
	}
	
	private final Node<E> UNKNOWN = new Node<E>(null, BLACK);
	
	final class ResultSet implements HGRandomAccessResult<E>
	{
	    boolean locked = false;
		int lookahead = 0;
		Node<E> next = UNKNOWN, current = UNKNOWN, prev = UNKNOWN;
		
		// Keeps track of parents of current node because Node itself doesn't have
		// a parent field.
		NodeStack stack = new NodeStack();  
		
		// min, max, advance, back all work on the current position as 
		// stored in the 'stack' of parents.
		//
		// 'min' returns the smallest element rooted at (and including)
		// stack.top while 'max' analogously returns the largest element
		//
		// 'advance' returns the next smallest elements and 'back' the previous
		// largest element
		//
		// All 4 modify the stack so that it's positioned at the returned
		// node
		
		Node<E> min()
		{
			Node<E> result = stack.top();
			while (result.left != null)
				result = stack.push(result.left);
			return result;
		}

		Node<E> max()
		{
			Node<E> result = stack.top();
			while (result.right != null)
				result = stack.push(result.right);
			return result;
		}
		
		Node<E> advance()
		{
			Node<E> current = stack.top();
			if (current.right != null)
			{
				stack.push(current.right);
				return min();
			}
			else
			{
				stack.backup();
				stack.pop();
				while (!stack.isEmpty())
				{
					Node<E> parent = stack.top();
					if (parent.left == current)
						return parent;
					else
						current = stack.pop();
				}
				stack.restore();
				return null;
			}
		}
		
		Node<E> back()
		{
			Node<E> current = stack.top();
			if (current.left != null)
			{
				stack.push(current.left);
				return max();
			}
			else
			{
				stack.backup();
				stack.pop();
				while (!stack.isEmpty())
				{
					Node<E> parent = stack.top();
					if (parent.right == current)
						return parent;
					else
						current = stack.pop();
				}
				stack.restore();
				return null;
			}
		}
		
		ResultSet(boolean acquireLock)
		{
		    if (acquireLock)
		        lock.readLock().lock();
		    locked = acquireLock;
		}
		
		public void goBeforeFirst()
		{
		    lookahead = 0;
	        next = current = prev = UNKNOWN;	        
	        stack.clear(); 		    
		}
		
		public void goAfterLast()
		{
            lookahead = 0;
            stack.clear();          
            stack.push(root);            
            prev = max();            
            next = current = UNKNOWN;            
		}
		
		@SuppressWarnings("unchecked")
        public GotoResult goTo(E key, boolean exactMatch)
		{
			// Not clear here whether we should be starting from the root really?
			// Gotos are performed generally during result set intersection where the target
			// is expected to be approximately close to the current position. Anyway,
			// starting from the root simplifies the implementations so until profiling
			// reveals the need for something else we start from the root.
			
			// To make sure the current position remains unchanged if we return GotoResult.nothing
			// we save the stack array.
			stack.backup();
			stack.clear();
			Node<E> current = root; 
			GotoResult result = GotoResult.nothing;
			Comparable<E> ckey = providedComparator == null ? (Comparable<E>)key : null; // make typecast out of loop, expensive!
			while (current != null)
			{
				stack.push(current);
				int cmp = ckey == null ? providedComparator.compare(key, current.key) : ckey.compareTo(current.key);
				if (cmp == 0)
				{
					result = GotoResult.found;
					break;
				}
				else if (cmp < 0)
				{
					if (exactMatch || current.left != null)
						current = current.left;
					else
					{
						result = GotoResult.close;
						break;
					}
				}
				else
				{
					if (exactMatch || current.right != null)
						current = current.right;
					else if (advance() != null)
					{
						result = GotoResult.close;
						break;
					}
					else
						break;
				}
			}
			if (GotoResult.nothing == result)
			{
				stack.restore();
				return GotoResult.nothing;
			}
			else
			{
				lookahead = 0;
				next = UNKNOWN;
				prev = UNKNOWN;
				this.current = stack.top();
				return result;
			}
		}

		public void close()
		{
		    if (locked)
		        lock.readLock().unlock();
		}

		public E current()
		{
			if (current == UNKNOWN)
				throw new NoSuchElementException();
			else
				return current.key;
		}

		public boolean isOrdered()
		{
			return true;
		}

		public boolean hasNext()
		{			
			if (next == UNKNOWN)
				moveNext();
			return next != null;
		}

		// Advance internal cursor to next element and assign 'next' to it. 
		private void moveNext()
		{
			if (stack.isEmpty())
			{
				stack.push(root);
				next = min();
				lookahead = 1;
			}			
			else while (true)
	        {
	        	next = advance();
	        	if (next == null)
	        		break;
	        	if (++lookahead == 1)
	        		break;
	        }			
		}
		
		public E next()
		{
			if (!hasNext())
				throw new NoSuchElementException();
			prev = current;
			current = next;
	        lookahead--;
	        moveNext();
			return current.key;
		}

		public void remove()
		{			
            if (current == UNKNOWN)
                throw new NoSuchElementException();
	        // Because of lack of parent pointers in Node, we can't really
	        // take advantage of the fact that we are already positioned 
	        // at the node we want to delete. In the current iteration context,
	        // we could make use of the parent stack to some advantage, but this
	        // would require a completely new version of the delete algo which
	        // is too big of a price to pay.
	        //
	        // So we just do a normal remove and reset the iterator to its 'prev' state.
            LLRBTree.this.remove(current.key);
            if (prev != null)
                if (goTo(prev.key, true) == GotoResult.nothing)
                    throw new Error("LLRBTree.ResultSet.remove buggy.");
            else
            {
                current = prev = next = UNKNOWN;
                lookahead = 0;
                stack.clear();
            }
		}

		public boolean hasPrev()
		{
			if (prev == UNKNOWN)
				movePrev();
			return prev != null;
		}

		private void movePrev()
		{
			if (stack.isEmpty())
				prev = null;			
			else while (true)
	        {
	        	prev = back();
	        	if (prev == null)
	        		break;
	        	if (--lookahead == -1)
	        		break;
	        }			
		}
		
		public E prev()
		{
			if (prev == null)
				throw new NoSuchElementException();
			next = current;
			current = prev;
	        lookahead++;
	        movePrev();
			return current.key;
		}		
	}
	// END IMPLEMENTATION OF Iterator/HGSearchResult
	
	private Node<E> root = null;
	private int size = 0;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private Comparator<E> providedComparator = null;
	
	private static boolean isRed(Node<?> x)
	{
		return x == null ? false : x.color == RED;
	}
	
	private static boolean isBlack(Node<?> x)
	{
		return x == null || x.color == BLACK;
	}
	
	// A node is right-leaning if it's right child is red, but it's left is black
	private static boolean isRightLeaning(Node<?> x)
	{
		return x == null ?  false : isRed(x.right) && isBlack(x.left);
	}
	
	@SuppressWarnings("unchecked")
    private Node<E> insert(Node<E> h, E key)
	{
		if (h == null)
		{
			size++;
			return new Node<E>(key, RED);
		}
				
		// Split 4-Nodes
		if (isRed(h.left) && isRed(h.right))
			h.colorFlip();		
		
		int cmp = providedComparator != null ? providedComparator.compare(key, h.key)
		                                     : ((Comparable<E>)key).compareTo(h.key);
		          
		if (cmp < 0)
			h.left = insert(h.left, key);
		else if (cmp > 0)
			h.right = insert(h.right, key);
		
		// Fix right leaning tree.
		if (isRed(h.right) && isBlack(h.left))
			h = h.rotateLeft();		
		// Fix two reds in a row.
		else if (isRed(h.left) && isRed(h.left.left))
			h = h.rotateRight();
						
		return h;
	}
	
	private Node<E> min(Node<E> h)
	{
		if (h == null)
			return null;
		Node<E> x = h;
		while (x.left != null) 
			x = x.left;
		return x;		
	}	
	
	private Node<E> max(Node<E> h)
	{
		if (h == null)
			return null;
		Node<E> x = h;
		while (x.right != null)
			x = x.right;
		return x;		
	}
	
	// The following two functions are for debugging only to print to coloring of a node, its
	// children and its grandchildren.
/*	private String cs(boolean b) { return b ? "red" : "black"; }	
	private String colors(Node<E> h)
	{
		String colors = "h -> " + cs(h.color);
		if (h.left != null)
		{
			colors += ", h.left -> " + cs(h.left.color);
			if (h.left.left != null)
				colors += ", h.left.left -> " + cs(h.left.left.color);
			else
				colors += ", h.left.left = null";
			if (h.left.right != null)
				colors += ", h.left.right -> " + cs(h.left.right.color);
			else
				colors += ", h.left.right = null";
		}
		if (h.right != null)
		{
			colors += ", h.right -> " + cs(h.right.color);
			if (h.right.left != null)
				colors += ", h.right.left -> " + cs(h.right.left.color);
			else
				colors += ", h.right.left = null";
			if (h.right.right != null)
				colors += ", h.right.right -> " + cs(h.right.right.color);
			else
				colors += ", h.right.right = null";
		}	
		return colors;
	} */
	
	private Node<E> deleteMax(Node<E> h)
	{				
		if (isRed(h.left) && isBlack(h.right))
			h = h.rotateRight();
		else if (h.right == null)
			return null; // h.left will be null here as well 
		if (isBlack(h.right) && isBlack(h.right.left))
			h = h.moveRedRight();
		h.right = deleteMax(h.right);
		return h.fixUp();
	}	
	
	private Node<E> deleteMin(Node<E> h)
	{
		if (h.left == null)
			return null; // h.right will be null here as well
		if (isBlack(h.left) && isBlack(h.left.left))
			h = h.moveRedLeft();
		h.left = deleteMin(h.left);
		return h.fixUp();
	}
	
	@SuppressWarnings("unchecked")
    private Node<E> delete(Node<E> h, E key)
	{
		int cmp = providedComparator != null ? providedComparator.compare(key, h.key)
                                             : ((Comparable<E>)key).compareTo(h.key); 
		if (cmp < 0) 
		{
			if (!isRed(h.left) && !isRed(h.left.left))
				h = h.moveRedLeft();
			h.left =  delete(h.left, key);
		}
		else  // cmp >= 0
		{
			if (isRed(h.left) && isBlack(h.right)) 
			{
				h = h.rotateRight();
				cmp++; // if we rotate right, then current h becomes necessarily < key
			}
			else if (cmp == 0 && (h.right == null))
			{
				size--;
				return null; // h.left is null here due to transformations going down
			}
			Node<E> tmp = h; // track if moveRedRight changes 'h' so we don't need to call key.compareTo again
			if (!isRed(h.right) && !isRed(h.right.left))
				tmp = h.moveRedRight();
			// if no rotation in above line and key==h.key replace with successor and we're done
			if (tmp == h && cmp == 0)
			{
				h.key = min(h.right).key;
				h.right = deleteMin(h.right);
				size--;
			} 
			else
			{
				h = tmp;
				h.right = delete(h.right, key);
			}
		} 
		return h.fixUp();		
	}

	public LLRBTree()
	{
	}
	
	public LLRBTree(Comparator<E> comparator)
	{
		this.providedComparator = comparator;
	}
	
	public void removeMax()
	{
		lock.writeLock().lock();
		try
		{
			if (root == null)
				return;
			root = deleteMax(root);
			if (root != null)
				root.color = BLACK;
			size--;
		}
		finally		
		{
			lock.writeLock().unlock();
		}
	}
	
	public void removeMin()
	{
		lock.writeLock().lock();
		try
		{
			if (root == null)
				return;
			root = deleteMin(root);
			if (root != null)
				root.color = BLACK;
			size--;
		}
		finally		
		{
			lock.writeLock().unlock();
		}
	}	
	
	// Set interface implementation
	
	public int size() { return size; }
	public boolean isEmtpy() { return size == 0; }
	public Comparator<E> comparator() { return providedComparator; }
	
	public void clear()
	{
		lock.writeLock().lock();
		root = null;
		size = 0;
		lock.writeLock().unlock();
	}
	
	@SuppressWarnings("unchecked")
	public boolean contains(Object key)
	{
		lock.readLock().lock();
		try
		{
			Node<E> current = root;
			Comparable<E> ckey = providedComparator == null ? (Comparable<E>)key : null; 
			while (current != null)
			{
				int cmp = ckey != null ? ckey.compareTo(current.key) : providedComparator.compare((E)key, current.key);
				if (cmp == 0)
					return true;
				else if (cmp < 0)
					current = current.left;
				else
					current = current.right;
			}
			return false;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}
	
	public boolean add(E key)
	{
		lock.writeLock().lock();
		try
		{
			int s = size;
			root = insert(root, key);
			root.color = BLACK;
			return s != size; 
		}
		finally
		{
			lock.writeLock().unlock();
		}
	}
	
	@SuppressWarnings("unchecked")	
	public boolean remove(Object key)
	{
		lock.writeLock().lock();
		try
		{
			if (root == null)
				return false;
			int s = size;
			root = delete(root, (E)key);
			if (root != null)
				root.color = BLACK;
			return s != size;
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
			if (root == null)
				return null;
			return min(root).key;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}

	public E last()
	{
		lock.readLock().lock();
		try
		{
			if (root == null)
				return null;
			return max(root).key;
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
	
	public SortedSet<E> subSet(E fromElement, E toElement)
	{
		throw new UnsupportedOperationException("...because of lazy implementor: this is a TODO.");		
	}

	public SortedSet<E> tailSet(E fromElement)
	{
		throw new UnsupportedOperationException("...because of lazy implementor: this is a TODO.");
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterator<E> iterator()
	{
		if (isEmpty())
			return (Iterator<E>)HGSearchResult.EMPTY; // avoid checking for root == null in ResultSet impl.
		else
			return new ResultSet(false);
	}

	public HGRandomAccessResult<E> getSearchResult()
	{
		return new ResultSet(true);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Object clone() throws CloneNotSupportedException
	{
		lock.readLock().lock();
		try
		{
			LLRBTree<E> cl = (LLRBTree<E>)super.clone();		
			cl.root = root == null ? root : root.clone();
			cl.size = size;
			return cl;
		}
		finally
		{
			lock.readLock().unlock();
		}
	}	
	
	
	public int depth()
	{
		lock.readLock().lock();
		try
		{
			return depth(root);
		}
		finally
		{
			lock.readLock().unlock();
		}		
	}
	private int depth(Node<E> x)
	{
		if (x == null) return 0;
		else return Math.max(1 + depth(x.left), 1 + depth(x.right));
	}
	
	// Integrity checks... 
	public boolean check() 
	{
		return isBST() && is234() && isBalanced();
	}

	public boolean isBST() 
	{  // Is this tree a BST?
		return isBST(root, first(), last());
	}

    @SuppressWarnings("unchecked")
    private boolean isBST(Node<E> x, E min, E max)
    {  // Are all the values in the BST rooted at x between min and max,
      // and does the same property hold for both subtrees?
    	if (x == null) return true;
    	int c1 = providedComparator != null ? providedComparator.compare(x.key, min)
                                            : ((Comparable<E>)x.key).compareTo(min);
    	int c2 = providedComparator != null ? providedComparator.compare(max, x.key)
                                            : ((Comparable<E>)max).compareTo(x.key);
    	if (c1 < 0 || c2 < 0) return false;
    	return isBST(x.left, min, x.key) && isBST(x.right, x.key, max);
    } 

    public boolean is234() { return is234(root); }

	boolean is234(Node<E> x)
	{  
		if (x == null) return true;
		
		// Does the tree have no red right links, and at most two (left)
	    // red links in a row on any path?			
	    if (isRightLeaning(x))
	    {
	    	System.err.println("Right leaning node");
	    	return false;
	    }
	    if (isRed(x))
	      if (isRed(x.left))
	        {
	        	System.err.println("2 consecutive reds");
	        	return false;
	        }
	    return is234(x.left) && is234(x.right);
	} 
	
    public boolean isBalanced() { return isBalanced(root); }
    
	public boolean isBalanced(Node<E> r)
	{ // Do all paths from root to leaf have same number of black edges?
		int black = 0;     // number of black links on path from root to min
	    Node<E> x = r;
	    while (x != null)
	    {
	       if (!isRed(x)) black++;
	          x = x.left;
	    }
	    return isBalanced(r, black);
	 }

	 private boolean isBalanced(Node<E> x, int black)
	 { // Does every path from the root to a leaf have the given number 
	     // of black links?
	    if      (x == null && black == 0) return true;
	    else if (x == null && black != 0) return false;
	    if (!isRed(x)) black--;
	    return isBalanced(x.left, black) && isBalanced(x.right, black);
	 }
}
