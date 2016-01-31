package hgdbteststorage;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.type.javaprimitive.DoubleType;
import org.hypergraphdb.util.ArrayBasedSet;
import org.junit.After;
import org.junit.Before;

public class SortIndexTests extends StoreImplementationTestBase
{
	HGSortIndex<Double, HGPersistentHandle> index;
	ArrayBasedSet<Double> keys;

	
	@Before
	public void initIndex()
	{
		index = (HGSortIndex<Double, HGPersistentHandle>)impl().getIndex("testBidirectionalIndex",
				  			  new DoubleType(),				
							  BAtoHandle.getInstance(config().getHandleFactory()),  
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
