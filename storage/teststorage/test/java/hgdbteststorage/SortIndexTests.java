package hgdbteststorage;

import hgtest.T;
import hgtest.TestUtils;
import hgtest.verify.HGAssert;

import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.type.javaprimitive.DoubleType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SortIndexTests extends StoreImplementationTestBase
{
	HGSortIndex<Double, HGPersistentHandle> index;
	TreeMap<Double, TreeSet<HGPersistentHandle>> map;

	
	/**
	 * The index is populated with a random number (b/w 2-5000) of random doubles. Each
	 * key has a random number (b/w 1 and 10) of handles associated with it. 
	 */
	@Before
	public void initIndex()
	{
		index = (HGSortIndex<Double, HGPersistentHandle>)impl().getIndex("testBidirectionalIndex",
				  			  new DoubleType(),				
							  BAtoHandle.getInstance(config().getHandleFactory()),  
							  new DoubleType().getComparator(),
							  null,
							  false, 
							  true);
		map = new TreeMap<Double, TreeSet<HGPersistentHandle>>();
		// number of keys
		int count = T.random(2000, 5000);
		for (int i = 0; i < count; i++)
		{
			double value = Math.random() * T.random(Integer.MAX_VALUE);
			TreeSet<HGPersistentHandle> handles = new TreeSet<HGPersistentHandle>();
			// number of values per key
			int valueCount = T.random(1, 10);
			for (int j = 0; j < valueCount; j++)
			{
				HGPersistentHandle handle = hfactory().makeHandle();
				handles.add(handle);
				index.addEntry(value, handle);
			}
			map.put(value, handles);
		}
	}

	@After
	public void clearIndex()
	{
		impl().removeIndex(index.getName());
	}
	
	@Test
	public void findLTTest()
	{
		// There is nothing less than the smallest number
		double smallest = map.firstKey();
		try (HGSearchResult<HGPersistentHandle> rs = index.findLT(smallest - 0.00000001))
		{
			Assert.assertFalse(rs.hasNext());
		}		
		try (HGSearchResult<HGPersistentHandle> rs = index.findLT(smallest))
		{
			Assert.assertFalse(rs.hasNext());
		}
		int cnt = T.random(1,  map.size());
		double Nth = map.keySet().stream().skip(cnt).iterator().next();
		try (HGSearchResult<HGPersistentHandle> rs = index.findLT(Nth))
		{
			TreeSet<HGPersistentHandle> all = new TreeSet<HGPersistentHandle>();
			map.keySet().stream().limit(cnt).forEach(d -> all.addAll(map.get(d)));
			HGAssert.assertSetEquals(all, TestUtils.set(rs));
		}		 
	}
	
	@Test
	public void findLTETest()
	{
		double dkey = map.firstKey();
		try (HGSearchResult<HGPersistentHandle> rs = index.findLTE(dkey))
		{			
			HGAssert.assertSetEquals(map.get(dkey), TestUtils.set(rs));
		}
		dkey = map.lastKey();
		try (HGSearchResult<HGPersistentHandle> rs = index.findLTE(dkey))
		{			
			Set<HGPersistentHandle> all = map.values().stream().flatMap(
					set->set.stream()).collect(Collectors.toSet());			
			HGAssert.assertSetEquals(all, TestUtils.set(rs));
		}		
	}
	
	@Test
	public void findGTTest()
	{
		double largest = map.lastKey();
		try (HGSearchResult<HGPersistentHandle> rs = index.findGT(largest + 0.00000001))
		{
			Assert.assertFalse(rs.hasNext());
		}		
		try (HGSearchResult<HGPersistentHandle> rs = index.findGT(largest))
		{
			Assert.assertFalse(rs.hasNext());
		}
		int cnt = T.random(1,  map.size());
		double Nth = map.keySet().stream().skip(cnt - 1).iterator().next();
		try (HGSearchResult<HGPersistentHandle> rs = index.findGT(Nth))
		{
			TreeSet<HGPersistentHandle> all = new TreeSet<HGPersistentHandle>();
			map.keySet().stream().skip(cnt).forEach(d -> all.addAll(map.get(d)));
			Set<HGPersistentHandle> S2  = TestUtils.set(rs);
			HGAssert.assertSetEquals(all, S2);
		}			
	}
	
	@Test
	public void findGTETest()
	{
		double dkey = map.lastKey();
		try (HGSearchResult<HGPersistentHandle> rs = index.findGTE(dkey))
		{			
			HGAssert.assertSetEquals(map.get(dkey), TestUtils.set(rs));
		}
		dkey = map.firstKey();
		try (HGSearchResult<HGPersistentHandle> rs = index.findGTE(dkey))
		{			
			Set<HGPersistentHandle> all = map.values().stream().flatMap(
						set->set.stream()).collect(Collectors.toSet());
			HGAssert.assertSetEquals(all, TestUtils.set(rs));
		}		
	}	
}