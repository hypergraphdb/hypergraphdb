package hgdbteststorage;

import org.hypergraphdb.HGIndex;
import org.hypergraphdb.storage.BAtoBA;
import org.junit.Assert;
import org.junit.Test;

public class EmtpyIndexTest extends StoreImplementationTestBase
{
	@Test public void emptyIndex()
	{
		String name = "testEmptyIndex";
		HGIndex<byte[], byte[]> empty = impl().getIndex(name, 
													  BAtoBA.getInstance(), 
													  BAtoBA.getInstance(), 
													  null,
													  null,
													  false, 
													  true);
		Assert.assertEquals(0, empty.count());
		empty.close();
		empty.open();
		Assert.assertEquals(name, empty.getName());
		impl().removeIndex(name);
		Assert.assertNull(impl().getIndex(name));
	}
}
