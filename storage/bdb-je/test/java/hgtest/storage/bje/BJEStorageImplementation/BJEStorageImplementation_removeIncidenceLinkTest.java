package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 */
public class BJEStorageImplementation_removeIncidenceLinkTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void handleIsNull() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to update incidence set for handle null: java.lang.NullPointerException");

		startup();
		final HGPersistentHandle handle = null;
		final HGPersistentHandle link = new UUIDPersistentHandle();
		try
		{
			storage.removeIncidenceLink(handle, link);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void bothLinkAndHandleAreNull() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to update incidence set for handle null: java.lang.NullPointerException");

		startup();
		try
		{
			storage.removeIncidenceLink(null, null);
		}
		catch (Exception occurred)
		{
			assertExceptions(occurred, expected);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void linkIsNull() throws Exception
	{
		startup();
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = null;
		try
		{
			storage.removeIncidenceLink(handle, link);
		}
		catch (Exception ex)
		{
			final String expectedMessage = String
					.format("Failed to update incidence set for handle %s: java.lang.NullPointerException",
							handle);
			assertExceptions(ex, HGException.class, expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}

	@Test
	public void thereIsOneLink() throws Exception
	{
		startupWithAdditionalTransaction(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		final HGPersistentHandle link = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, link);

        storage.removeIncidenceLink(handle, link);

        final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemove.hasNext());
		afterRemove.close();
		shutdown();
	}

	@Test
	public void thereAreTwoLinks() throws Exception
	{
		startupWithAdditionalTransaction(5);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle third = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.addIncidenceLink(first, third);

        storage.removeIncidenceLink(first, second);
		storage.removeIncidenceLink(first, third);

        final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemove.hasNext());
		afterRemove.close();
		shutdown();
	}

	@Test
	public void thereAreNotStoredLinks() throws Exception
	{
		startup(2);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();

        storage.removeIncidenceLink(first, second);

        final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemove.hasNext());
		afterRemove.close();
		shutdown();
	}

	@Test
	public void incidenceLinkIsLinkedToItself() throws Exception
	{
		startupWithAdditionalTransaction(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.addIncidenceLink(handle, handle);

        storage.removeIncidenceLink(handle, handle);

        final HGRandomAccessResult<HGPersistentHandle> afterRemove = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemove.hasNext());
		shutdown();
	}
}
