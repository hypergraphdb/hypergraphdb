package hgtest.storage.HGStorageImplementation;

import hgtest.TestUtils;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.util.HGUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * @author Yuiy Sechko
 */
public class HGStorageImplementation_addIncidenceLinkTest extends
		HGStorageImplementationTestBasis
{
	@Test(dataProvider = "configurations_2")
	public void addOneLink(final Class configuration) throws Exception
	{
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
        initSpecificStorageImplementation(configuration);

		storage.addIncidenceLink(first, second);

		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(first);
		assertEquals(storedLinks.next(), second);
		storedLinks.close();
	}

	@Test(dataProvider = "configurations_4")
	public void addSeveralLinks(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };

		initSpecificStorageImplementation(configuration);
        storage.addIncidenceLink(handle, links[0]);
		storage.addIncidenceLink(handle, links[1]);
		storage.addIncidenceLink(handle, links[2]);

		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(handle);
		assertEquals(TestUtils.set(storedLinks), HGUtils.set(links));
		storedLinks.close();
	}

	@Test(dataProvider = "configurations_2")
	public void addLinkToItself(final Class configuration) throws Exception
	{
		final HGPersistentHandle handle = new UUIDPersistentHandle();

		initSpecificStorageImplementation(configuration);
		storage.addIncidenceLink(handle, handle);

		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(handle);
		assertEquals(storedLinks.next(), handle);
		storedLinks.close();
	}
}
