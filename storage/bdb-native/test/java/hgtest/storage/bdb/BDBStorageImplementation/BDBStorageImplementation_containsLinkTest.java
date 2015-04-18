package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
 */
public class BDBStorageImplementation_containsLinkTest extends
		BDBStorageImplementationTestBasis
{

	@Test
	public void checkExistenceOfStoredLinkFromFirstToSecond() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		assertTrue(storage.containsLink(first));
		shutdown();
	}

	@Test
	public void checkExistenceOfStoredLinkFromSecondToFirst() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] { second };
		storage.store(first, links);
		assertFalse(storage.containsLink(second));
		shutdown();
	}

	@Test
	public void checkExistenceOfHandleWhichIsLinkedToItself() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new HGPersistentHandle[] { handle });
		assertTrue(storage.containsLink(handle));
		shutdown();
	}

	@Test
	public void checkExistenceOfNonStoredLink() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		assertFalse(storage.containsLink(handle));
		shutdown();
	}

	@Test
	public void checkExistenceOfLinkUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.containsLink(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertNull(ex.getMessage());
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void arrayOfStoredLinksIsEmpty() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new HGPersistentHandle[] {});
		assertTrue(storage.containsLink(handle));
		shutdown();
	}
}
