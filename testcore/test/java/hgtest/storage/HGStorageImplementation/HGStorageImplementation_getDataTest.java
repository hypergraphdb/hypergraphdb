package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_getDataTest extends HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_1")
	public void readDataWhichIsNotStored(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final byte[] retrieved = storage.getData(handle);
		assertNull(retrieved);
	}

	@Test(dataProvider = "configurations_2")
	public void readEmptyArray(final Class configuration) throws Exception
	{
		final byte[] expected = new byte[] {};

        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void readArrayWhichContainsOneItem(final Class configuration)
			throws Exception
	{
		final byte[] expected = new byte[] { 44 };

        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 44 });
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void readArrayWhichContainsSeveralItems(final Class configuration)
			throws Exception
	{
		final byte[] expected = new byte[] { 11, 22, 33 };

        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 11, 22, 33 });
		final byte[] retrieved = storage.getData(handle);
		assertEquals(retrieved, expected);
	}
}
