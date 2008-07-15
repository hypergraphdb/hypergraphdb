package hgtest.benchmark;

import hgtest.T;

import java.util.*;
import org.hypergraphdb.atom.impl.*;
import org.hypergraphdb.*;
import org.hypergraphdb.HGRandomAccessResult.GotoResult;

public class LLRBTest
{
	static void populate(SortedSet<HGPersistentHandle> S, int size)
	{
		for (int i = 0; i < size; i++)
			S.add(HGHandleFactory.makeHandle());
	}
	
	static void compare(TreeSet<HGPersistentHandle> baseSet, LLRBTree<HGPersistentHandle> tree)
	{
		System.out.println("LLRB tree check: " + tree.check());
		Iterator<HGPersistentHandle> i = baseSet.iterator(), j = tree.iterator();
		while (i.hasNext())
		{
			if (!j.hasNext())
				throw new RuntimeException("Missing element from tree.");
			else if (i.next().compareTo(j.next()) != 0)
				throw new RuntimeException("Wrong other b/w baseSet and tree.");
		}
		System.out.println("Base set and LLRBTree set are the same.");		
	}
	
	static void randomWalk(LLRBTree<HGPersistentHandle> tree, List<HGPersistentHandle> L, int iterations)
	{
		HGRandomAccessResult<HGPersistentHandle> rs = tree.getSearchResult();
		rs.next();
		HGPersistentHandle max = tree.last();
		
		for (int i = 0; i < iterations; i++)
		{
			// Find existing:
			HGPersistentHandle h = L.get(T.random(L.size()));
			if (rs.goTo(h, true) != GotoResult.found)
				throw new RuntimeException("Can't find element " + h + " on iteration " + i);
			T.backAndForth(rs, 100, 10);
			
			// Find missing with exact match:
			HGPersistentHandle current = (HGPersistentHandle)rs.current();
			h = HGHandleFactory.makeHandle();
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
	}
	
	public static void main(String [] argv)
	{
		TreeSet<HGPersistentHandle> baseSet = new TreeSet<HGPersistentHandle>();
		populate(baseSet, 100000);
		LLRBTree<HGPersistentHandle> tree = new LLRBTree<HGPersistentHandle>();
		List<HGPersistentHandle> list = new ArrayList<HGPersistentHandle>();
		list.addAll(baseSet);
		T.shuffle(list);
		tree.addAll(list);
		System.out.println("Tree size:" + tree.size() + ", Tree depth: " + tree.depth());
		compare(baseSet, tree);
		HGRandomAccessResult rs = tree.getSearchResult();
		rs.next();
		System.out.println("Going back and forth...");
//		T.backAndForth(rs, 10, 1000000);
		System.out.println("Doing a random walk...");
		randomWalk(tree, list, 100000);
		System.out.println("Done.");
	}
}
