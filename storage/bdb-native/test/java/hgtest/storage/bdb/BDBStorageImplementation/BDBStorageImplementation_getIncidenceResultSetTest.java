package hgtest.storage.bdb.BDBStorageImplementation;

import hgtest.TestUtils;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.util.HGUtils;
import org.testng.annotations.Test;

import static hgtest.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 *
 */
public class BDBStorageImplementation_getIncidenceResultSetTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void useNullHandle() throws Exception
	{
		final Exception expected = new NullPointerException(
				"HGStore.getIncidenceSet called with a null target handle.");

		startup();
		try
		{
			storage.getIncidenceResultSet(null);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
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
		assertEquals(TestUtils.set(incidence), HGUtils.set(links));
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

    // TODO make comparing exception message with wildcards or something similar
    @Test(enabled = false)
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
					.format("Failed to retrieve incidence set for %s: java.lang.IllegalStateException: Exception in test case.",
                            storage);
			assertEquals(ex.getMessage(), expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
