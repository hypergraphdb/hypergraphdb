package hgtest.storage.HGStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_removeIncidenceSetTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_3")
	public void thereIsOneLinkInIncidenceSet(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);

		storage.removeIncidenceSet(first);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
	}

	@Test(dataProvider = "configurations_4")
	public void thereAreSeveralLinksInIncidenceSet(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle third = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.addIncidenceLink(first, third);

		storage.removeIncidenceSet(first);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
	}

	@Test(dataProvider = "configurations_2")
	public void removeIncidenceSetForLinkWhichIsNotStored(
			final Class configuration) throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		storage.removeIncidenceSet(handle);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
	}

	@Test(dataProvider = "configurations_3")
	public void thereAreNotIncidenceLinks(final Class configuration)
			throws Exception
	{
		initSpecificStorageImplementation(configuration);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});

		storage.removeIncidenceSet(handle);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
	}
}
