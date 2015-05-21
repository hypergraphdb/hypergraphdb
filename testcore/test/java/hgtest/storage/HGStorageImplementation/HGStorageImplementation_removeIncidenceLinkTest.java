package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_removeIncidenceLinkTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_transaction_3")
	public void thereIsOneLink(final Class configuration) throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, link);

		storage.removeIncidenceLink(handle, link);
		final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemove.hasNext());
		afterRemove.close();
	}

	@Test(dataProvider = "configurations_transaction_5")
	public void thereAreTwoLinks(final Class configuration) throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle third = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.addIncidenceLink(first, third);

		storage.removeIncidenceLink(first, second);
		storage.removeIncidenceLink(first, third);
		final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemove.hasNext());
		afterRemove.close();
	}

	@Test(dataProvider = "configurations_2")
	public void thereAreNotStoredLinks(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();

		storage.removeIncidenceLink(first, second);
		final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemove.hasNext());
		afterRemove.close();
	}

	@Test(dataProvider = "configurations_transaction_3")
	public void incidenceLinkIsLinkedToItself(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, handle);

		storage.removeIncidenceLink(handle, handle);
		final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemove.hasNext());
        afterRemove.close();
	}
}
