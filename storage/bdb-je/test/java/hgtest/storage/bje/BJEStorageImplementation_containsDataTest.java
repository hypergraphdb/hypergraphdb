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
public class BJEStorageImplementation_containsDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void checkExistenceOfStoredDataUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.containsData(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(), null);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void checkExistenceOfStoredData() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		assertTrue(storage.containsData(handle));
		shutdown();
	}

	@Test
	public void checkExistenceOfNonStoredData() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		assertFalse(storage.containsData(handle));
		shutdown();
	}
}
