package hgtest.storage.bje;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;


/**
 */
public class BJEStorageImplementation_storeTest extends BJEStorageImplementationTestBasis {

	@Test
	public void storeAndReadDataUsingHandle() throws Exception
	{
		final byte[] expected = new byte[] { 4, 5, 6 };
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
		shutdown();
	}
}
