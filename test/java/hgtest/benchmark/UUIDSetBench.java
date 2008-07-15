package hgtest.benchmark;

import java.util.*;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.atom.HGAtomSet;

/**
 * 
 * <p>
 * Test the performance of a set data structure for storing UUIDPersistentHandles.
 * </p>
 *
 * @author Borislav Iordanov
 *
 */
public class UUIDSetBench
{
	private static double lookupAll(Set<HGHandle> baseSet, HGAtomSet destination)
	{
		long start = System.currentTimeMillis();
//		int cnt = 0;
		for (int i = 0; i < 30; i++)
		for (HGHandle x : baseSet)
		{
			destination.contains(x);
			destination.contains(HGHandleFactory.makeHandle());
/*			System.out.println(x);
			if (cnt % 100 == 0)
				System.out.println("cnt=" + cnt);
			cnt++; */
		}
		return (System.currentTimeMillis() - start)/1000.0;		
	}
	
	private static double addAll(Set<HGHandle> baseSet, Set<HGHandle> destination)
	{
		long start = System.currentTimeMillis();
		for (HGHandle x : baseSet)
			destination.add(x);
		return (System.currentTimeMillis() - start)/1000.0;		
	}

	private static double lookupAll(Set<HGHandle> baseSet, Set<HGHandle> destination)
	{
		long start = System.currentTimeMillis();
//		int cnt = 0;
		for (int i = 0; i < 30; i++)
		for (HGHandle x : baseSet)
		{
			destination.contains(x);
			destination.contains(HGHandleFactory.makeHandle());
/*			System.out.println(x);
			if (cnt % 100 == 0)
				System.out.println("cnt=" + cnt);
			cnt++; */
		}
		return (System.currentTimeMillis() - start)/1000.0;		
	}
	
	public static void main(String []argv)
	{
		HashSet<HGHandle> baseSet = new HashSet<HGHandle>();
		for (int i = 0; i < 10000; i++)
		{
			HGPersistentHandle h = HGHandleFactory.makeHandle();
			baseSet.add(h);
		}
		
		HGAtomSet set = new HGAtomSet();
		System.out.println("atomset add:" + addAll(baseSet, set));
		System.out.println("atomset lookup:" + lookupAll(baseSet, set));
		TreeSet<HGHandle> tset = new TreeSet<HGHandle>();
		System.out.println("treeset add:" + addAll(baseSet, tset));
		System.out.println("treeset lookup:" + lookupAll(baseSet, tset));
	}
}