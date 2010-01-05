/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.atom.impl;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.Stack;
import org.hypergraphdb.util.Pair;

/**
 * <p>An implementation of a trie for storing UUIDs. This is used to efficiently 
 * represent persistent handle sets.
 * </p>
 * 
 * <p>
 * Elements of this trie are assumed to be of fixed 16 byte length. The trie is implemented
 * with a 16 length alphabet (each byte is split into two parts of 4 bits). Thus the depth of the 
 * resulting tree structure is 32. Terminal elements are the ones that reach that depth.
 * </p>
 *  
 * <p>
 * This implementation also supports a compact and efficient serialization of the whole
 * structure for storage purposes.
 * </p>
 * 
 * <p>
 * Since this is intended as an implementation of a <code>HGAtomSet</code>, which is 
 * statically typed for persistent handles, no checks are made for nulls or
 * correct byte array sizes.
 * </p>
 * 
 * <p>
 * Purging of branches is implemented during lookup. If the lookup procedure reaches a
 * death branch (i.e. one with all its children pointers set to null), that branch is removed.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public final class UUIDTrie 
{
	//
	// The trie structure is represented by a small nested class hierarchy. 
	//
	// We type internal and leaf nodes separately because leaf nodes don't need
	// to hold an array of children. We can do this simple optimization since all
	// our elements are of fixed length...
	//
	
	private static class trie implements Cloneable { public Object clone() { return this; } }	
	private static final class leaf_trie extends trie 
		{ public Object clone() { return THE_LEAF; } }
	
	// a single instance representing leafs is enough since they don't store anything.
	private static final leaf_trie THE_LEAF = new leaf_trie();
	
	// there is an optimization opportunity here to eliminate the recursive calls.
	private static final class node_trie extends trie
	{
		trie [] children = new trie[16];
		byte count = 0; // number of non-null children

		public Object clone() 
		{ 
			node_trie cl = new node_trie();
			cl.count = count;
			cl.children = new trie[children.length];
			for (int i = 0; i < children.length; i++)
				cl.children[i] = (trie)(((trie)children[i]).clone());
			return cl;
		}
		
		//
		// The following recursive version of find was forgotten in favor of
		// a non-recursive version (below). However, this recursive version has
		// the advantage of performing a more aggressive pruning of death nodes.
		// If it turns out that atom sets are very volatile, and that aggressive 
		// freeing up of unused nodes is needed, this recursive version might
		// be preferred. Or implement a separate pruning method (called 'compact' 
		// or something...).
		//
/*		boolean find(byte [] uuid, int offset)
		{
			byte current = uuid[offset / 2];
			current = (offset % 2 == 0) ? (byte) ((current & 0xF0) >> 4) : (byte)(current & 0x0F);
			
			if (offset == 31)
				return children[current] != null;
			
			node_trie child = (node_trie)children[current];
			
			if (child == null)
				return false;
			
			boolean result = child.find(uuid, offset + 1);
			
			if (child.count == 0)
			{
				children[current]= null;
				count--;
			}
			
			return result; 				
		} */
		
/*		boolean remove(byte [] uuid, int offset)
		{
			byte current = uuid[offset / 2];
			current = (offset % 2 == 0) ? (byte) ((current & 0xF0) >> 4) : (byte)(current & 0x0F);
			if (children[current] == null)
				return false;
			else if (offset == 31)
			{
				children[current] = null;
				count--;
				return true;
			}
			else
				return children[current].find(uuid, offset + 1);			
		} */
		
/*		boolean add(byte [] uuid, int offset)
		{
			byte current = uuid[offset / 2];
			current = (offset % 2 == 0) ? (byte) ((current & 0xF0) >> 4) : (byte)(current & 0x0F);
			if (children[current] == null)
			{
				count++;
				if (offset == 31)
				{
					children[current] = THE_LEAF;
					return false;
				}
				else
				{
					node_trie child = new node_trie();
					children[current] = child;
					return child.add(uuid, offset + 1);					
				}
			}
			else
			{
				if (offset == 31)
					return true;
				else
					return children[current].add(uuid, offset + 1);
			}
		} */
	}

	private node_trie root = new node_trie();
	
	public void clear()
	{
		root = new node_trie();
	}
	
	public UUIDTrie clone()
	{
		UUIDTrie trie = new UUIDTrie();
		trie.root = (node_trie)root.clone();
		return trie;
	}
	
	/**
	 * <p>Add a new element returning <code>true</code> if it wasn't already in the set
	 * and <code>false</code> otherwise.</p>
	 * 
	 * @param uuid
	 * @return
	 */
	public boolean add(byte [] uuid)
	{
		node_trie node = root;
		byte offset = 0;
		byte position;
		while (true)
		{
			if (offset % 2 == 0)
			{
				position = (byte)(uuid[offset >> 1] >> 4);
				if (position < 0)
					position = (byte)(-position + 7);
			}
			else
				position = (byte)(uuid[offset >> 1] & 0x0F);
			if (node.children[position] == null)
			{
				node.count++;
				if (offset == 31)
				{
					node.children[position] = THE_LEAF;
					return true;
				}
				else
				{
					node = (node_trie)(node.children[position] = new node_trie());
					offset++;			
				}
			}
			else
			{
				if (offset == 31)
					return false;
				else
				{
					node = (node_trie)node.children[position];
					offset++;
				}
			}
		}
	}

	/**
	 * <p>Return <code>true</code> if the given element is in the set and <code>false</code>
	 * otherwise.</p>
	 * 
	 * @param uuid
	 * @return
	 */
	public boolean find(byte [] uuid)
	{
		node_trie node = root;
		byte position;
		byte offset = 0;
		while (true)
		{
			if (offset % 2 == 0)
			{
				position = (byte)(uuid[offset >> 1] >> 4);
				if (position < 0)
					position = (byte)(-position + 7);				
			}
			else
				position = (byte)(uuid[offset >> 1] & 0x0F);
					
			if (offset == 31)
				return node.children[position] != null;
			
			node_trie child = (node_trie)node.children[position];
			
			if (child == null)
				return false;
					
			if (child.count == 0)
			{
				node.children[position]= null;
				node.count--;
				return false;
			}			
			node = child;
			offset++;
		}
	}
	
	/**
	 * <p>Remove an element and return <code>true</code> if it was present, and
	 * <code>false</code. otherwise.</p>
	 *  
	 * @param uuid
	 * @return
	 */
	public boolean remove(byte [] uuid)
	{
		node_trie node = root;
		byte offset = 0;
		byte position;
		while (true)
		{
			if (offset % 2 == 0)
			{
				position = (byte)(uuid[offset >> 1] >> 4);
				if (position < 0)
					position = (byte)(-position + 7);				
			}
			else
				position = (byte)(uuid[offset >> 1] & 0x0F);
			if (node.children[position] == null)
				return false;
			else if (offset == 31)
			{
				node.children[position] = null;
				node.count--;
				return true;
			}
			else
			{
				node = (node_trie)node.children[position];
				offset++;
			}
		}		
	}
	
	private void serialize(ByteArrayOutputStream out, node_trie node, int depth)
	{
		byte i, layout;
		int bit;		
		for (i = 0, layout = 0, bit = 1; i < 8; i++, bit*=2) 
			if (node.children[i] != null) layout |= bit;
		out.write(layout);
		for (i = 8, layout = 0, bit = 1; i < 16; i++, bit*=2)
			if (node.children[i] != null) layout |= bit;
		out.write(layout);
		if (depth < 31)
			for (i = 0; i < 16; i++)
				if (node.children[i] != null)
					serialize(out, (node_trie)node.children[i], depth + 1);
	}
	
	public byte [] serialize()
	{
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		serialize(out, root, 0);
		return out.toByteArray();
	}
	
	private void deserialize(ByteArrayInputStream in, node_trie node, int depth)
	{
		byte layout1 = (byte)in.read();
		byte layout2 = (byte)in.read();
		byte i, bit;
		
		if (depth < 31)
		{
			for (i = 0, bit = 1; i < 8; i++, bit *= 2)
				if  ((layout1 & bit) != 0)
				{
					node.count++;
					node_trie child = new node_trie();
					node.children[i] = child;
					deserialize(in, child, depth + 1);
				}
			for (i = 8, bit = 1; i < 16; i++, bit *= 2)
				if  ((layout2 & bit) != 0)
				{
					node.count++;
					node_trie child = new node_trie();
					node.children[i] = child;
					deserialize(in, child, depth + 1);
				}
		}
		else
		{
			for (i = 0, bit = 1; i < 8; i++, bit *= 2)
				if  ((layout1 & bit) != 0)
				{
					node.count++;
					node.children[i] = THE_LEAF;
				}
			for (i = 8, bit = 1; i < 16; i++, bit *= 2)
				if  ((layout2 & bit) != 0)
				{
					node.count++;
					node.children[i] = THE_LEAF;
				}
		}
	}
	
	public void deserialize(byte [] data)
	{
		ByteArrayInputStream in = new ByteArrayInputStream(data);
		for (byte i = 0; i < 16; i++)
			root.children[i] = null;
		deserialize(in, root, 0);		
	}
	
	public Iterator<byte[]> iterator()
	{
		return new TrieIterator();
	}
	
	private class TrieIterator implements Iterator<byte[]>
	{
		// a stack that holds the state of what would be a recursive traversal
		Stack<Pair<node_trie, Integer>> state = new Stack<Pair<node_trie, Integer>>();
		
		void goToNext()
		{
			while (!state.isEmpty())
			{
				Pair<node_trie, Integer> top = state.pop();
				node_trie n = top.getFirst();
				int i = top.getSecond();
				do { i++; } while (i < n.children.length && n.children[i] == null);
				if (i < n.children.length)
				{
					state.push(new Pair<node_trie, Integer>(n, i));
					if (state.size() < 32)
						state.push(new Pair<node_trie, Integer>((node_trie)n.children[i], -1));
					else
						break;
				}
			}
		}
		
		public TrieIterator()
		{
			state.push(new Pair<node_trie, Integer>(root, -1));
			goToNext();
		}
		
		public boolean hasNext()
		{			
			return !state.isEmpty();
		}

		public byte[] next()
		{	
			// if we have a next element, then we must be already positionned at a leaf
			// in the trie, so we just read off the value from the current stack, which must
			// be precisely 32 elements deep
			byte [] result = null;
			if (hasNext())
			{
				result = new byte[16];
				int idx = 0;
				for (Iterator<Pair<node_trie, Integer>> i = state.iterator(); i.hasNext(); )
				{
					byte high = i.next().getSecond().byteValue();
					if (high > 7)
						high = (byte)(7 - high);
					byte low = i.next().getSecond().byteValue();
					result[idx++] = (byte)(16*high + low);
				}
				goToNext();
			}
			return result;
		}

		public void remove()
		{			
			if (state.size() < 16)
				throw new IllegalStateException("TrieIterator.remove: the iterator has no current object to remove.");
			else if (state.peek().getSecond() < 0) 
				throw new IllegalStateException("TrieIterator.remove: the iterator has no current object to remove.");
			else
			{
				Pair<node_trie,Integer> curr = state.peek();
				curr.getFirst().children[curr.getSecond()] = null;
				curr.getFirst().count--;
				while (curr.getFirst().count == 0)
				{
					state.pop();
					if (state.isEmpty())
						break;
					curr = state.peek();
					curr.getFirst().children[curr.getSecond()] = null;
					curr.getFirst().count--;
				}
			}
		}
	}
}
