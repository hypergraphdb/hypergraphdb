package hgtest.storage.HGStorageImplementation;

import hgtest.TestUtils;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.util.HGUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 * @author Yuriy Sechko
 */
public class HGStorageImplementation_getIncidenceResultSetTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_1")
	public void noIncidenceLinksAreStored(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();

		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(handle);

		assertFalse(incidence.hasNext());
		incidence.close();
	}

	@Test(dataProvider = "configurations_2")
	public void oneIncidenceLinkIsStored(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);

		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(first);

		assertEquals(incidence.next(), second);
		incidence.close();
	}

	@Test(dataProvider = "configurations_4")
	public void severalIncidenceLinksAreStored(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };
		storage.addIncidenceLink(first, links[0]);
		storage.addIncidenceLink(first, links[1]);
		storage.addIncidenceLink(first, links[2]);

		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(first);

		assertEquals(TestUtils.set(incidence), HGUtils.set(links));
		incidence.close();
	}

	@Test(dataProvider = "configurations_2")
	public void checkLinksFromSecondToFirst(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);

		final HGRandomAccessResult<HGPersistentHandle> incidenceFromSecondToFirst = storage
				.getIncidenceResultSet(second);

		assertFalse(incidenceFromSecondToFirst.hasNext());
		incidenceFromSecondToFirst.close();
	}

	@Test(dataProvider = "configurations_2")
	public void handleIsLinkedToItself(final Class configuration)
			throws Exception
	{
        initSpecificStorageImplementation(configuration);

		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, handle);

		HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(handle);

		assertEquals(incidence.next(), handle);
		incidence.close();
	}
}
