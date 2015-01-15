package hgtest.storage.bje.BJEStorageImplementation;

import hgtest.storage.bje.TestUtils;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.util.HGUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 */
public class BJEStorageImplementation_addIncidenceLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void linkIsNull() throws Exception
	{
		startup();
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = null;
		try
		{
			storage.addIncidenceLink(handle, link);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to update incidence set for handle %s: java.lang.NullPointerException",
							handle);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void addOneLink() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(first);
		assertEquals(storedLinks.next(), second);
		storedLinks.close();
		shutdown();
	}

	@Test
	public void addSeveralLinks() throws Exception
	{
		startup(4);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };
		storage.addIncidenceLink(handle, links[0]);
		storage.addIncidenceLink(handle, links[1]);
		storage.addIncidenceLink(handle, links[2]);
		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(handle);
		assertEquals(TestUtils.set(storedLinks), HGUtils.set(links));
		storedLinks.close();
		shutdown();
	}

	@Test
	public void addLinkToItself() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, handle);
		final HGRandomAccessResult<HGPersistentHandle> storedLinks = storage
				.getIncidenceResultSet(handle);
		assertEquals(storedLinks.next(), handle);
		storedLinks.close();
		shutdown();
	}
}
