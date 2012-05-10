package hgtest.benchmark;

import java.util.*;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.atom.HGAtomSet;
import org.hypergraphdb.atom.impl.UUIDTrie;
import org.hypergraphdb.handle.UUIDHandleFactory;

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
	private static double lookupAll(HGHandleFactory handleFactory, Set<HGPersistentHandle> baseSet, HGAtomSet destination)
	{
		long start = System.currentTimeMillis();
//		int cnt = 0;
		for (int i = 0; i < 30; i++)
		for (HGHandle x : baseSet)
		{
			destination.contains(x);
			destination.contains(handleFactory.makeHandle());
/*			System.out.println(x);
			if (cnt % 100 == 0)
				System.out.println("cnt=" + cnt);
			cnt++; */
		}
		return (System.currentTimeMillis() - start)/1000.0;		
	}
	
	private static double addAll(Set<HGPersistentHandle> baseSet, Set<HGHandle> destination)
	{
		long start = System.currentTimeMillis();
		for (HGPersistentHandle x : baseSet)
			destination.add(x);
		return (System.currentTimeMillis() - start)/1000.0;		
	}

	private static double lookupAll(HGHandleFactory handleFactory, Set<HGPersistentHandle> baseSet, Set<HGHandle> destination)
	{
		long start = System.currentTimeMillis();
//		int cnt = 0;
		for (int i = 0; i < 30; i++)
		for (HGPersistentHandle x : baseSet)
		{
			destination.contains(x);
			destination.contains(handleFactory.makeHandle());
/*			System.out.println(x);
			if (cnt % 100 == 0)
				System.out.println("cnt=" + cnt);
			cnt++; */
		}
		return (System.currentTimeMillis() - start)/1000.0;		
	}
	
    
    private static double addAll(Set<HGPersistentHandle> baseSet, UUIDTrie destination)
    {
        long start = System.currentTimeMillis();
        for (HGPersistentHandle x : baseSet)
            destination.add(x.toByteArray());
        return (System.currentTimeMillis() - start)/1000.0;     
    }

    private static double lookupAll(HGHandleFactory handleFactory, Set<HGPersistentHandle> baseSet, UUIDTrie destination)
    {
        long start = System.currentTimeMillis();
//      int cnt = 0;
        for (int i = 0; i < 30; i++)
        for (HGPersistentHandle x : baseSet)
        {
            destination.find(x.toByteArray());
            destination.find(handleFactory.makeHandle().toByteArray());
/*          System.out.println(x);
            if (cnt % 100 == 0)
                System.out.println("cnt=" + cnt);
            cnt++; */
        }
        return (System.currentTimeMillis() - start)/1000.0;     
    }
    
	public static void main(String []argv)
	{
		HashSet<HGPersistentHandle> baseSet = new HashSet<HGPersistentHandle>();
		HGHandleFactory handleFactory = new UUIDHandleFactory();
		for (int i = 0; i < 10000; i++)
		{
			HGPersistentHandle h = handleFactory.makeHandle();
			baseSet.add(h);
		}
        SortedSet<HGHandle> tset = Collections.synchronizedSortedSet(new TreeSet<HGHandle>());
        System.out.println("treeset add:" + addAll(baseSet, tset));
        System.out.println("treeset lookup:" + lookupAll(handleFactory, baseSet, tset));
		HGAtomSet set = new HGAtomSet();
		System.out.println("atomset add:" + addAll(baseSet, set));
		System.out.println("atomset lookup:" + lookupAll(handleFactory, baseSet, set));
	}
}