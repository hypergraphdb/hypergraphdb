package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 */
public class BJEStorageImplementation_removeDataTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void removeArrayWhichContainsSeveralItems() throws Exception
	{
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 1, 2, 3 });
		storage.removeData(handle);
		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
		shutdown();
	}

	@Test
	public void removeEmptyArray() throws Exception
	{
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		storage.removeData(handle);
		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
		shutdown();
	}

	@Test
	public void removeArrayWhichContainsOneItem() throws Exception
	{
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 2 });
		storage.removeData(handle);
		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
		shutdown();
	}

	@Test
	public void removeDataWhichIsNotStored() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.removeData(handle);
		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
		shutdown();
	}

	@Test
	public void removeDataUsingAnotherHandle() throws Exception
	{
		final byte[] expected = new byte[] { 11, 22, 33 };
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle anotherHandle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 11, 22, 33 });
		storage.removeData(anotherHandle);
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
	}

	@Test
	public void useNullHandle() throws Exception
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

	@Test
	public void exceptionWhileRemovingData() throws Exception
	{
		startup(new IllegalStateException("Throw exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.removeData(handle);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to remove value with handle %s: java.lang.IllegalStateException: Throw exception in test case.",
							handle);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
