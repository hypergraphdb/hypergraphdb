package hgtest.storage.bje.BJEStorageImplementation;

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
	public void useNullHandle() throws Exception
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
	public void thereAreNotIncidenceLinks() throws Exception
	{
		startup(1);
		final long cardinality = storage
				.getIncidenceSetCardinality(new UUIDPersistentHandle());
		assertEquals(cardinality, 0);
		shutdown();
	}

	@Test
	public void thereIsOneIncidenceLink() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 1);
		shutdown();
	}

	@Test
	public void thereAreSeveralIncidenceLinks() throws Exception
	{
		startup(4);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		storage.addIncidenceLink(handle, new UUIDPersistentHandle());
		final long cardinality = storage.getIncidenceSetCardinality(handle);
		assertEquals(cardinality, 3);
		shutdown();
	}

	@Test
	public void exceptionIsThrown() throws Exception
	{
		startup(new IllegalArgumentException("Exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.getIncidenceSetCardinality(handle);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to retrieve incidence set for handle %s: java.lang.IllegalArgumentException: Exception in test case.",
							handle);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
