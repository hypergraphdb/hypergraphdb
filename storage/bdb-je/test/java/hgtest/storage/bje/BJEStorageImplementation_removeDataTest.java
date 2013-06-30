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
public class BJEStorageImplementation_removeDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void removeDataUsingHandle() throws Exception
	{
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 1, 2, 3 });
		storage.removeData(handle);
		final byte[] retrievedData = storage.getData(handle);
		assertNull(retrievedData);
		shutdown();
	}

	@Test
	public void removeDataUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.removeData(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(),
					"HGStore.remove called with a null handle.");
		}
		finally
		{
			shutdown();
		}
	}
}
