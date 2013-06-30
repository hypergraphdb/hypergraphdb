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
public class BJEStorageImplementation_removeIncidenceSetTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void removeIncidenceSetUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.removeIncidenceSet(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			assertEquals(ex.getMessage(),
					"Failed to remove incidence set of handle null: java.lang.NullPointerException");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void removeIncidenceSetWhichContainsOneLink() throws Exception
	{
		startup(3);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.removeIncidenceSet(first);
		final long afterRemoving = storage.getIncidenceSetCardinality(first);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

	@Test
	public void removeIncidenceSetWhichContainsTwoLinks() throws Exception
	{
		startup(4);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle third = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.addIncidenceLink(first, third);
		storage.removeIncidenceSet(first);
		final long afterRemoving = storage.getIncidenceSetCardinality(first);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

	@Test
	public void removeIncidenceSetForLinkWhichIsNotStored() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.removeIncidenceSet(handle);
		final long afterRemoving = storage.getIncidenceSetCardinality(handle);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

	@Test
	public void removeIncidenceSetForLinkWhichHasNotIncidenceLinks()
			throws Exception
	{
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		storage.removeIncidenceSet(handle);
		final long afterRemoving = storage.getIncidenceSetCardinality(handle);
		assertEquals(afterRemoving, 0);
		shutdown();
	}

}
