package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_getLinkTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_1")
	public void getLinkWhichIsNotStored(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final HGPersistentHandle[] storedLinks = storage.getLink(handle);
		assertNull(storedLinks);
	}

	@Test(dataProvider = "configurations_2")
	public void getOneStoredLink(final Class configuration) throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] { new UUIDPersistentHandle() };

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { expected[0] };
		storage.store(first, links);

		final HGPersistentHandle[] stored = storage.getLink(first);
		assertEquals(stored, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void getSeveralStoredLinks(final Class configuration)
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				expected[0], expected[1], expected[2] };
		storage.store(first, links);

		final HGPersistentHandle[] stored = storage.getLink(first);
		assertEquals(stored, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void getEmptyArrayOfLinks(final Class configuration)
			throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {};

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new HGPersistentHandle[] {});

		final HGPersistentHandle[] stored = storage.getLink(handle);
		assertEquals(stored, expected);
	}
}
