package hgdbteststorage;

import java.util.Arrays;


import hgtest.T;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.HGIndexStats;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class IndexInterfaceTests extends StoreImplementationTestBase
{
	HGIndex<HGPersistentHandle, byte[]> index;
	
	@Before
	public void initIndex()
	{
		index = impl().getIndex("testSimpleIndex", 
							  BAtoHandle.getInstance(config().getHandleFactory()), 
							  BAtoBA.getInstance(), 
							  null,
							  null,
							  false, 
							  true);
	}

	@After
	public void clearIndex()
	{
		impl().removeIndex(index.getName());
	}
		
	@Test(expected=HGException.class) 
	public void testClose()
	{
		index.close();
		index.stats().entries(Long.MAX_VALUE, false).value();
	}
	
	@Test public void addEntries()
	{
		HGPersistentHandle key = config().getHandleFactory().makeHandle();
		byte [] value = "hi".getBytes();
		index.addEntry(key, value);
		Assert.assertArrayEquals(index.findFirst(key), value);		
		HGIndexStats.Count cnt = index.stats().keys(Long.MAX_VALUE, false);
		if (cnt != null)
			Assert.assertEquals(1, cnt.value());
		cnt = index.stats().valuesOfKey(key, Long.MAX_VALUE, false);
		Assert.assertEquals(1, cnt.value());
	}

	@Test public void addMultiValuedEntries()
	{
		HGPersistentHandle key = config().getHandleFactory().makeHandle();
		int valuesCount = T.random(50, 200);
		byte [][] values = new byte[valuesCount][];
		for (int i = 0; i < valuesCount; i++)
		{
			values[i] = T.randomBytes(T.random(10000));
			index.addEntry(key, values[i]);
		}

		// duplicate values associated with a given key must be sorted 
		Arrays.sort(values, T::compareByteArrays);
		Assert.assertArrayEquals(index.findFirst(key), values[0]);		
		try (HGRandomAccessResult<byte[]> rs = index.find(key))
		{
			for (byte [] current : values)
			{
				Assert.assertTrue(rs.hasNext());
				Assert.assertArrayEquals(current, rs.next());
			}
		}
		// check counts make sense
		HGIndexStats.Count cnt = index.stats().keys(Long.MAX_VALUE, false);
		if (cnt != null)
			Assert.assertEquals(1, cnt.value());
		cnt = index.stats().valuesOfKey(key, Long.MAX_VALUE, false);
		Assert.assertEquals(valuesCount, cnt.value());
	}
}