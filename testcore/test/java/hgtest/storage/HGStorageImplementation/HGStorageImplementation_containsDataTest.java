package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_containsDataTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_2")
	public void arrayOfSeveralItems(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		initSpecificStorageImplementation(configuration);
		storage.store(handle, new byte[] { 4, 5, 6 });

		assertTrue(storage.containsData(handle));
	}

	@Test(dataProvider = "configurations_2")
	public void emptyArray(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		initSpecificStorageImplementation(configuration);
		storage.store(handle, new byte[] {});

		assertTrue(storage.containsData(handle));
	}

	@Test(dataProvider = "configurations_2")
	public void arrayOfOneItem(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		initSpecificStorageImplementation(configuration);
		storage.store(handle, new byte[] { 1 });

		assertTrue(storage.containsData(handle));
	}

	@Test(dataProvider = "configurations_1")
	public void dataIsNotStored(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		initSpecificStorageImplementation(configuration);
		assertFalse(storage.containsData(handle));
	}
}
