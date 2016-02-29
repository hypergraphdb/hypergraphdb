package hgdbteststorage;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.BAtoHandle;
import org.junit.After;
import org.junit.Before;

public class BiIndexTests extends StoreImplementationTestBase
{
	HGBidirectionalIndex<HGPersistentHandle, byte[]> index;
	
	@Before
	public void initIndex()
	{
		index = (HGBidirectionalIndex<HGPersistentHandle, byte[]>)impl().getIndex("testBidirectionalIndex", 
							  BAtoHandle.getInstance(config().getHandleFactory()), 
							  BAtoBA.getInstance(), 
							  null,
							  null,
							  true, 
							  true);
	}

	@After
	public void clearIndex()
	{
		impl().removeIndex(index.getName());
	}

}
