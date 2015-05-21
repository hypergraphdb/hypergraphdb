package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_storeTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_2")
	public void storeArrayOfBytes(final Class configuration) throws Exception
	{
		final byte[] expected = new byte[] { 4, 5, 6 };

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void storeEmptyArray(final Class configuration) throws Exception
	{
		final byte[] expected = new byte[] {};

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void storeArrayOfOneByte(final Class configuration) throws Exception
	{
		final byte[] expected = new byte[] { 22 };

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 22 });
		final byte[] stored = storage.getData(handle);
		assertEquals(stored, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void arrayOfLinksIsEmpty(final Class configuration) throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {};

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new HGPersistentHandle[] {});
		final HGPersistentHandle[] stored = storage.getLink(handle);
		assertEquals(stored, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void storeOneLink(final Class configuration) throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] { new UUIDPersistentHandle() };

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { expected[0] };
		storage.store(handle, links);
		final HGPersistentHandle[] stored = storage.getLink(handle);
		assertEquals(stored, expected);
	}

	@Test(dataProvider = "configurations_2")
	public void storeSeveralLinks(final Class configuration) throws Exception
	{
		final HGPersistentHandle[] expected = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };

		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				expected[0], expected[1], expected[2] };
		storage.store(handle, links);
		final HGPersistentHandle[] stored = storage.getLink(handle);
		assertEquals(stored, expected);
	}
}
