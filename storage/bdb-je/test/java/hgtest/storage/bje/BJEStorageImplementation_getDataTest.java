package hgtest.storage.bje;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

/**
 */
public class BJEStorageImplementation_getDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void readDataWhichIsNotStored() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
		shutdown();
	}

	@Test
	public void useNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.getData(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(ex.getMessage(),
					"Failed to retrieve link with handle null");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void readEmptyArray() throws Exception
	{
		final byte[] expected = new byte[] {};
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
		shutdown();
	}

	@Test
	public void readArrayWhichContainsOneItem() throws Exception
	{
		final byte[] expected = new byte[] { 44 };
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 44 });
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
		shutdown();
	}

	@Test
	public void readArrayWhichContainsSeveralItems() throws Exception
	{
		final byte[] expected = new byte[] { 11, 22, 33 };
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 11, 22, 33 });
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
		shutdown();
	}
}
