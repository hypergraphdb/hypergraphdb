package hgtest.benchmark;

import hgtest.T;

import java.util.*;

import org.hypergraphdb.handle.UUIDHandleFactory;
import org.hypergraphdb.util.*;
import org.hypergraphdb.*;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;

@SuppressWarnings("unchecked")
public class LLRBTest
{
	static void populate(HGHandleFactory handleFactory, Collection<HGPersistentHandle> S, int size)
	{
		for (int i = 0; i < size; i++)
			S.add(handleFactory.makeHandle());
	}
	
	static void compare(TreeSet<HGPersistentHandle> baseSet, SortedSet<HGPersistentHandle> tree)
	{
		Iterator<HGPersistentHandle> i = baseSet.iterator(), j = tree.iterator();
		while (i.hasNext())
		{
			if (!j.hasNext())
				throw new RuntimeException("Missing element from tree.");
			else if (i.next().compareTo(j.next()) != 0)
				throw new RuntimeException("Wrong order b/w baseSet and tree.");
		}		
		((HGSearchResult)j).close();
	}
	
	static void randomWalk(HGHandleFactory handleFactory, SortedSet<HGPersistentHandle> set, List<HGPersistentHandle> L, int iterations)
	{
		HGRandomAccessResult<HGPersistentHandle> rs = (set instanceof LLRBTree) ? 
						((LLRBTree)set).getSearchResult() : ((ArrayBasedSet)set).getSearchResult();
		rs.next();
		HGPersistentHandle max = set.last();
		
		for (int i = 0; i < iterations; i++)
		{
			// Find existing:
			HGPersistentHandle h = L.get(T.random(L.size()));
			if (rs.goTo(h, true) != GotoResult.found)
				throw new RuntimeException("Can't find element " + h + " on iteration " + i);
			T.backAndForth(rs, 100, 10);
			
			// Find missing with exact match:
			HGPersistentHandle current = (HGPersistentHandle)rs.current();
			h = handleFactory.makeHandle();
			if (rs.goTo(h, true) != GotoResult.nothing)
				throw new RuntimeException("oops, shouldn't have found " + h);
			else if (current != rs.current())
				throw new RuntimeException("goto returned nothing on handle " + h + 
						" but changed current from " + current + " to " + rs.current());
			
			// Find missing without exact match:
			GotoResult r = rs.goTo(h, false);
			if (r == GotoResult.close)
			{
				// check that previous < h < current
				if (h.compareTo(rs.current()) >= 0)
					throw new RuntimeException("goTo return close on " + h + " which is >= current");
				else if (rs.hasPrev() && h.compareTo(rs.prev()) <= 0)
					throw new RuntimeException("goTo return close on " + h + " which is <= previous");
				T.backAndForth(rs, 100, 10);
			}
			else if (r == GotoResult.nothing && h.compareTo(max) <= 0)
				throw new RuntimeException("goTo returned nothing on " + h + " which is <= max=" + max);
			else if (r == GotoResult.found)
				throw new RuntimeException("Shouldn't have found " + h);
		}
		rs.close();
	}

	
	public static void testRemovals(LLRBTree<HGPersistentHandle> tree, 
									TreeSet<HGPersistentHandle> baseSet, 
									List<HGPersistentHandle> list,
									int removeCount)
	{
/*		// removal of min
		list.remove(baseSet.first());
		baseSet.remove(baseSet.first());
		tree.removeMin();
		if (!tree.check())
		{
			System.out.println("BST=" + tree.isBST() + ", balanced=" + tree.isBalanced() + 
					", 234=" + tree.is234());
			throw new RuntimeException("RB-tree doesn't check after removal of min.");
		}
		compare(baseSet, tree); 
		
		// removal of max
		list.remove(baseSet.last());
		baseSet.remove(baseSet.last());
		tree.removeMax();
		if (!tree.check())
			throw new RuntimeException("RB-tree doesn't check after removal of max.");
		compare(baseSet, tree); */
		
		// removal of some random elements
		for (int i = 0; i < removeCount; i++)
		{
			int pos = T.random(list.size());
			HGPersistentHandle h = list.get(pos);
			list.remove(pos);
			baseSet.remove(h);
			if (!tree.remove(h))
				throw new RuntimeException("LLRBTRee.remove returned false on an existing element.");
/*			if (!tree.check())
			{
				System.out.println("BST=" + tree.isBST() + ", balanced=" + tree.isBalanced() + 
						", 234=" + tree.is234());				
				throw new RuntimeException("RB-tree check failed after removal of " + h);
			} */
			compare(baseSet, tree);			
		} 
	}

	
	public static void testArrayRemovals(ArrayBasedSet<HGPersistentHandle> tree, 
									TreeSet<HGPersistentHandle> baseSet, 
									List<HGPersistentHandle> list,
									int removeCount)
	{		
		// removal of some random elements
		for (int i = 0; i < removeCount; i++)
		{
			int pos = T.random(list.size());
			HGPersistentHandle h = list.get(pos);
			list.remove(pos);
			baseSet.remove(h);
			if (!tree.remove(h))
				throw new RuntimeException("ArrayBasedSet.remove returned false on an existing element.");
			compare(baseSet, tree);			
		} 
	}

	
	static void testLLRBTree(HGHandleFactory handleFactory, int iterations)
	{
		for (int n = 0; n < iterations; n++)
		{
			TreeSet<HGPersistentHandle> baseSet = new TreeSet<HGPersistentHandle>();
			populate(handleFactory, baseSet, 10000);
			LLRBTree<HGPersistentHandle> tree = new LLRBTree<HGPersistentHandle>();
			List<HGPersistentHandle> list = new ArrayList<HGPersistentHandle>();
			list.addAll(baseSet);
			T.shuffle(list);
			tree.addAll(list);
			System.out.println("Tree size:" + tree.size() + ", Tree depth: " + tree.depth());
			compare(baseSet, tree);
			HGRandomAccessResult<HGPersistentHandle> rs = tree.getSearchResult();
			rs.next();
			System.out.println("Going back and forth...");
			T.backAndForth(rs, 10, 10000);
			rs.close();			
			System.out.println("Doing a random walk...");
			randomWalk(handleFactory, tree, list, 1000);
			System.out.println("Test removals...");
			testRemovals(tree, baseSet, list, 20);
			System.out.println("Test insert-remove-insert-etc. several times...");
			for (int i = 0; i < 10; i++)
			{
				populate(handleFactory, list, 20);
				baseSet.addAll(list);
				tree.addAll(list);
/*				if (!tree.check())
				{
					System.out.println("BST=" + tree.isBST() + ", balanced=" + tree.isBalanced() + 
							", 234=" + tree.is234());				
					throw new RuntimeException("RB-tree check failed after remove & insert");					
				} */
				compare(baseSet, tree);
				randomWalk(handleFactory, tree, list, 1000);				
				testRemovals(tree, baseSet, list, 18);
			}
			System.out.println("Loopback " + n);
		}		
	}
	
	static void testArraySet(int iterations)
	{
	    HGHandleFactory handleFactory = new UUIDHandleFactory();
		for (int n = 0; n < iterations; n++)
		{
			TreeSet<HGPersistentHandle> baseSet = new TreeSet<HGPersistentHandle>();
			populate(handleFactory, baseSet, 10000);
			ArrayBasedSet<HGPersistentHandle> array = new ArrayBasedSet<HGPersistentHandle>(new HGPersistentHandle[0]);
			List<HGPersistentHandle> list = new ArrayList<HGPersistentHandle>();
			list.addAll(baseSet);
			T.shuffle(list);
			array.addAll(list);
			System.out.println("Array size: " + array.size());
			compare(baseSet, array);
/*			HGRandomAccessResult<HGPersistentHandle> rs = array.getSearchResult();
			rs.next();
			System.out.println("Going back and forth...");
			T.backAndForth(rs, 10, 10000);
			System.out.println("Doing a random walk...");
			randomWalk(array, list, 1000);
			System.out.println("Test removals..."); */
			HGPersistentHandle h = array.first();
			array.remove(h);
			baseSet.remove(h);
			list.remove(list.indexOf(h));
			compare(baseSet, array);
			testArrayRemovals(array, baseSet, list, 20);
			System.out.println("Test insert-remove-insert-etc. several times...");
			for (int i = 0; i < 10; i++)
			{
				populate(handleFactory, list, 20);
				baseSet.addAll(list);
				array.addAll(list);
				compare(baseSet, array);
				randomWalk(handleFactory, array, list, 1000);				
				testArrayRemovals(array, baseSet, list, 18);
			}
			System.out.println("Loopback " + n);
		}			
	}
	
	public static void main(String [] argv)
	{
		testArraySet(20);
		// testLLRBTree(20);
	}
}
