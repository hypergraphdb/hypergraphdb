package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_removeDataTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_3")
	public void removeArrayWhichContainsSeveralItems(final Class configuration) throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 1, 2, 3 });

		storage.removeData(handle);

		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
	}

    @Test(dataProvider = "configurations_3")
	public void removeEmptyArray(final Class configuration) throws Exception
	{
        initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});

		storage.removeData(handle);

		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
	}

    @Test(dataProvider = "configurations_3")
	public void removeArrayWhichContainsOneItem(final Class configuration) throws Exception
	{
        initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 2 });

		storage.removeData(handle);

		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
	}

    @Test(dataProvider = "configurations_2")
	public void removeDataWhichIsNotStored(final Class configuration) throws Exception
	{
        initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		storage.removeData(handle);

		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
	}

    @Test(dataProvider = "configurations_3")
	public void removeDataUsingAnotherHandle(final Class configuration) throws Exception
	{
		final byte[] expected = new byte[] { 11, 22, 33 };

        initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle anotherHandle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 11, 22, 33 });

		storage.removeData(anotherHandle);

		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
	}
}
