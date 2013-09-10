package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

/**
 */
public class BJEStorageImplementation_getIncidenceResultSetTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void useNullHandle() throws Exception
	{
		startup();
		try
		{
			storage.getIncidenceResultSet(null);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), NullPointerException.class);
			assertEquals(ex.getMessage(),
					"HGStore.getIncidenceSet called with a null handle.");
		}
		shutdown();
	}

	@Test
	public void noIncidenceLinksAreStored() throws Exception
	{
		startup(1);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(handle);
		assertFalse(incidence.hasNext());
		incidence.close();
		shutdown();
	}

	@Test
	public void oneIncidenceLinkIsStored() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(first);
		assertEquals(incidence.next(), second);
		incidence.close();
		shutdown();
	}

	@Test
	public void severalIncidenceLinksAreStored() throws Exception
	{
		startup(4);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle[] links = new HGPersistentHandle[] {
				new UUIDPersistentHandle(), new UUIDPersistentHandle(),
				new UUIDPersistentHandle() };
		storage.addIncidenceLink(first, links[0]);
		storage.addIncidenceLink(first, links[1]);
		storage.addIncidenceLink(first, links[2]);
		final HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(first);
		assertEquals(set(incidence), set(links));
		incidence.close();
		shutdown();
	}

	@Test
	public void checkLinksFromSecondToFirst() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		final HGRandomAccessResult<HGPersistentHandle> incidenceFromSecondToFirst = storage
				.getIncidenceResultSet(second);
		assertFalse(incidenceFromSecondToFirst.hasNext());
		incidenceFromSecondToFirst.close();
		shutdown();
	}

	@Test
	public void handleIsLinkedToItself() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, handle);
		HGRandomAccessResult<HGPersistentHandle> incidence = storage
				.getIncidenceResultSet(handle);
		assertEquals(incidence.next(), handle);
		incidence.close();
		shutdown();
	}

	@Test
	public void exceptionIsThrown() throws Exception
	{
		startup(new IllegalStateException("Exception in test case."));
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		try
		{
			storage.getIncidenceResultSet(handle);
		}
		catch (Exception ex)
		{
			assertEquals(ex.getClass(), org.hypergraphdb.HGException.class);
			final String expectedMessage = String
					.format("Failed to retrieve incidence set for handle %s: java.lang.IllegalStateException: Exception in test case.",
							handle);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
