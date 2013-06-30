package hgtest.storage.bje;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 */
public class BJEStorageImplementation_getLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void getLinksUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.getLink(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(ex.getMessage(),
					"Failed to retrieve link with handle null");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void getLinkWhichIsNotStored() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle[] storedLinks = storage.getLink(handle);
		assertNull(storedLinks);
		shutdown();
	}

	@Test
	public void storeOneLink() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		final HGPersistentHandle[] storedLinks = storage.getLink(first);
		assertEquals(storedLinks, links);
		shutdown();
	}

	@Test
	public void storeTwoLinks() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle() };
		storage.store(first, links);
		final HGPersistentHandle[] storedLinks = storage.getLink(first);
		assertEquals(storedLinks, links);
		shutdown();
	}
}
