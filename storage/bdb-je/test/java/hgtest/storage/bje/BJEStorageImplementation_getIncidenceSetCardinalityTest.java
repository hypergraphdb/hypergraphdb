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
public class BJEStorageImplementation_getIncidenceSetCardinalityTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void getIncidenceSetCardinalityUsingNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.getIncidenceSetCardinality(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(),
					"HGStore.getIncidenceSetCardinality called with a null handle.");
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void noIncidenceLinksForNonStoredHandle() throws Exception
	{
		startup(1);
		final long cardinality = storage
				.getIncidenceSetCardinality(new UUIDPersistentHandle());
		assertEquals(cardinality, 0);
		shutdown();
	}

	@Test
	public void createOneIncidenceLink() throws Exception
	{
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] { 4, 5, 6 });
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 1);
		shutdown();
	}

	@Test
	public void createTwoIncidenceLinks() throws Exception
	{
		startup(4);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 2);
		shutdown();
	}

}
