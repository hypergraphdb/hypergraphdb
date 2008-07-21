package org.hypergraphdb.atom.impl;

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
 * doesn't doesn't contain a pointer to a value, nor does it contains a pointer
 * to the parent which should make it more memory efficient than most
 * implementations (e.g. the standard java.util.TreeMap). However, tree
 * mutations are implemented recursively, which is not optimal and could
 * be removed in the future. 
 * </p>
 *
 * @author Borislav Iordanov
 *
 * @param <E> The type of elements this set stores. It must implement the <code>Comparable</code>
 * interface.
 */
public class LLRBTree<E extends Comparable<E>> extends AbstractSet<E>
											   implements SortedSet<E>, 
											   			  Cloneable, 
											   			  java.io.Serializable
{
	private static final long serialVersionUID = -1;
	
	private static final boolean RED = true;
	private static final boolean BLACK = false;

	// The node stack is used to keep track of parent pointers
	// during tree traversals. A fixed-size array is used for
	// the stack because it is known that it'll never go beyond
	// the depth of the tree which, since this a balanced tree,
	// is going to always be a relatively small number approx. 
	// equal to log(treeSize).
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
		
		boolean is234()
		{  
			// Does the tree have no red right links, and at most two (left)
		    // red links in a row on any path?			
		    if (isRightLeaning(this))
		    {
		    	System.err.println("Right leaning node");
		    	return false;
		    }
		    if (isRed(this))
		      if (isRed(left))
		        {
		        	System.err.println("2 consecutive reds");
		        	return false;
		        }
		    return (left == null || left.is234()) && (right == null || right.is234());
		} 		
	}
	
	private final Node<E> UNKNOWN = new Node<E>(null, BLACK);
	
	final class ResultSet implements HGRandomAccessResult<E>
	{
		int lookahead = 0;
		Node<E> next = UNKNOWN, current = UNKNOWN, prev = UNKNOWN;
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
		
		ResultSet()
		{
		}
		
		@SuppressWarnings("unchecked")
		public GotoResult goTo(E key, boolean exactMatch)
		{
			// Not clear here whether we should be starting from the root really?
			// Gotos are performed generally during result intersection where the target
			// is expected to be approximately close to the current position. Anyway,
			// starting from the root simplifies the implementations so until profiling
			// reveals the need for something else we start from the root.
			
			// To make sure the current position remains unchanged if we return GotoResult.nothing
			// we save the stack array.
			stack.backup();
			stack.clear();
			Node<E> current = root; 
			GotoResult result = GotoResult.nothing;
			while (current != null)
			{
				stack.push(current);
				int cmp = key.compareTo(current.key);
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
			// nope
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
			throw new UnsupportedOperationException();
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
	
	private Node<E> root = null;
	private int size = 0;
	private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	
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
		
		int cmp = key.compareTo(h.key);
		if (cmp == 0)
			; // already in set, nothing to do		
		else if (cmp < 0)
			h.left = insert(h.left, key);
		else
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
	
	private String cs(boolean b) { return b ? "red" : "black"; }
	
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
	}
	
	private Node<E> deleteMax(Node<E> h)
	{				
//		System.out.println("Enter DMAX " + colors(h));
		if (isRed(h.left) && isBlack(h.right))
		{
//			System.out.println("Rotate left-leaning");
			h = h.rotateRight();
		}
		else if (h.right == null)
		{
			if (h.left != null)
				h.left.color = h.color;
//			System.out.println("Delete at bottom");
			return h.left;
		}		
//		System.out.println("Continue lookup");
		if (isBlack(h.right) && isBlack(h.right.left))
		{
//			System.out.println("Move red right");
			h = h.moveRedRight();
		}
//		if (h != root && isRed(h) && isRed(h.right) || isRed(h.right) && isRed(h.right.right))
//			throw new RuntimeException("red red");
		h.right = deleteMax(h.right);
		return h.fixUp();
/*		System.out.println("Fixing up :" + colors(h));		
		Node<E> fixed = h.fixUp();
		System.out.println("Fixed :" + colors(fixed));
		boolean color = fixed.color;
		fixed.color = BLACK;
		if (!fixed.is234())
			throw new RuntimeException("fixed is broken.");			
		else
		{
			fixed.color = color;
			System.out.println("Exit DMAX");
			return fixed;
		} */
	}	
	
	private Node<E> deleteMin(Node<E> h)
	{
//		System.out.println("Enter delete min");
		if (h.left == null)
		{
//			System.out.println("Deleting at bottom, return right=" + h.right);
			if (h.right != null)
				h.right.color = h.color;
			return h.right;
		}
		if (isBlack(h.left) && isBlack(h.left.left))
		{
//			System.out.println("Borrowing from siblings.");
			h = h.moveRedLeft();
/*			if (!h.is234())
			{
				throw new RuntimeException("oops, broke it on the way down.");
			} */
		}
//		System.out.println("Recurse deletemin");
		h.left = deleteMin(h.left);
/*		if (h.left != null && isRed(h.left.right))
		{
			System.err.println("failed 234 at " + h.key);
		} 
		System.out.println("Fixup resulting node: " + h.is234());
		*/
		Node<E> r = h.fixUp();
//		System.out.println("Exiting deletemin");
		return r;
	}
	
	private Node<E> delete(Node<E> h, E key)
	{
/*		int cmp = key.compareTo(h.key);
		if (cmp == 0) // found
		{
			System.out.println("Element Found!");
			if (h.right != null)
			{
				System.out.println("replacing with successor");
				if (h.right.color == BLACK && isBlack(h.right.left))
				{
					h = h.moveRedRight();
					return delete(h, key);
				}
				h.key = min(h.right).key;
				h.right = deleteMin(h.right);
				size--;
				Node<E> fixed = h.fixUp();
				if (!isBalanced(fixed))
					System.err.println("fixed is fucked");
			}
			else if (isRed(h.left))
			{
				System.out.println("no successor pushing red left down.");
				return delete(h.rotateRight(), key);
			}
			else
			{
				System.out.println("no successor, left is not red, returning...");				
				size--;
				if (h.left != null)
					h.left.color = h.color;
				return h.left;
			}
		}
		else if (cmp < 0)
		{
			if (h.left == null) // not found
				return h.fixUp();
			// Borrow from siblings to ensure we don't have 2 nodes (in the 2-3-4 isomorphism)
			else if (h.left.color == BLACK && isBlack(h.left.left)) 
				h = h.moveRedLeft();
			h.left = delete(h.left, key);
		}
		else
		{
			// lean red links to the right
			if (isRed(h.left) && isBlack(h.right)) 
				h = h.rotateRight();
			// else borrow from siblings to ensure we don't have 2 nodes
			else if (h.right.color == BLACK && isBlack(h.right.left))
				h = h.moveRedRight();
			h.right = delete(h.right, key);
		}
		return h.fixUp(); */
	      if (key.compareTo(h.key) < 0) 
	      {
	         if (!isRed(h.left) && !isRed(h.left.left))
	            h = h.moveRedLeft();
	         h.left =  delete(h.left, key);
	      }
	      else 
	      {
	         if (isRed(h.left) && isBlack(h.right)) 
	            h = h.rotateRight();
	         if (key.compareTo(h.key) == 0 && (h.right == null))
	         {
	        	 size--;
	        	 if (h.left != null)
	        		 h.left.color = h.color;
	            return h.left;
	         }
	         if (!isRed(h.right) && !isRed(h.right.left))
	            h = h.moveRedRight();
	         if (key.compareTo(h.key) == 0)
	         {
	            h.key = min(h.right).key;
	            h.right = deleteMin(h.right);
	            size--;
	         }
	         else h.right = delete(h.right, key);
	      }
	 
	      return h.fixUp();		
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
	public Comparator<E> comparator() { return null; }
	
	public void clear()
	{
		lock.writeLock().lock();
		root = null;
		size = 0;
		lock.writeLock().unlock();
	}
	
	public boolean contains(E key)
	{
		lock.readLock().lock();
		try
		{
			Node<E> current = root; 
			while (current != null)
			{
				int cmp = key.compareTo(current.key);
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
		// TODO Auto-generated method stub
		return null;
	}
	
	public SortedSet<E> subSet(E fromElement, E toElement)
	{
		// TODO Auto-generated method stub
		return null;
	}

	public SortedSet<E> tailSet(E fromElement)
	{
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Iterator<E> iterator()
	{
		if (isEmpty())
			return (Iterator<E>)HGSearchResult.EMPTY; // avoid checking for root == null in ResultSet impl.
		else
			return new ResultSet();
	}

	public HGRandomAccessResult<E> getSearchResult()
	{
		return new ResultSet();
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

    private boolean isBST(Node<E> x, E min, E max)
    {  // Are all the values in the BST rooted at x between min and max,
      // and does the same property hold for both subtrees?
    	if (x == null) return true;
    	if (x.key.compareTo(min) < 0 || max.compareTo(x.key) < 0) return false;
    	return isBST(x.left, min, x.key) && isBST(x.right, x.key, max);
    } 

    public boolean is234() { return root == null || root.is234(); }


    public boolean isBalanced() { return isBalanced(root); }
    
	public boolean isBalanced(Node r)
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

	 private boolean isBalanced(Node x, int black)
	 { // Does every path from the root to a leaf have the given number 
	     // of black links?
	    if      (x == null && black == 0) return true;
	    else if (x == null && black != 0) return false;
	    if (!isRed(x)) black--;
	    return isBalanced(x.left, black) && isBalanced(x.right, black);
	 }
}