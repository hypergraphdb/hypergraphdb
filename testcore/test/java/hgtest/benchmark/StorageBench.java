package hgtest.benchmark;

import org.hypergraphdb.HGHandleFactory;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;

import hgtest.HGTestBase;

public class StorageBench extends HGTestBase
{
	public void singleValueUUID(int count)
	{
		HGStore store = graph.getStore();
		ByteArrayConverter<HGPersistentHandle> conv = BAtoHandle.getInstance(graph.getHandleFactory());
		if (store.getIndex("benchUUIDs", conv, conv, null, null, false) != null)
				store.removeIndex("benchUUIDs");
		HGIndex<HGPersistentHandle, HGPersistentHandle> idx = store.getIndex("benchUUIDs", 
					   BAtoHandle.getInstance(graph.getHandleFactory()), 
					   BAtoHandle.getInstance(graph.getHandleFactory()), 
					   null,
					   null,
					   true);
		HGHandleFactory hFactory = graph.getHandleFactory();
		long start = System.currentTimeMillis();
		for (int i = 0; i < count; i++)
		{
			idx.addEntry(hFactory.makeHandle(), hFactory.makeHandle());
		}
		System.out.println("" + count + "," + (System.currentTimeMillis() - start)/1000.0);
	}
	
	public static void main(String [] argv)
	{
		StorageBench bench = new StorageBench();
		//bench.config.setStoreImplementation(new BJEStorageImplementation());
		bench.setUp();
		try
		{
			for (int cnt = 1000; cnt < 1000000; cnt += 100000)
				bench.singleValueUUID(cnt);
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}
		finally
		{
			bench.tearDown();
		}
	}
}
