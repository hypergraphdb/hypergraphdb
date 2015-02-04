package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

/**
 */
public class BJEStorageImplementation_removeIncidenceSetTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void useNullHandle() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to remove incidence set of handle null: java.lang.NullPointerException");

		startup();
		try
		{
			storage.removeIncidenceSet(null);
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
	public void thereIsOneLinkInIncidenceSet() throws Exception
	{
		startup(3);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.removeIncidenceSet(first);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
		shutdown();
	}

	@Test
	public void thereAreSeveralLinksInIncidenceSet() throws Exception
	{
		startup(4);
		final HGPersistentHandle first = new UUIDPersistentHandle();
		final HGPersistentHandle second = new UUIDPersistentHandle();
		final HGPersistentHandle third = new UUIDPersistentHandle();
		storage.addIncidenceLink(first, second);
		storage.addIncidenceLink(first, third);
		storage.removeIncidenceSet(first);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(first);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
		shutdown();
	}

	@Test
	public void removeIncidenceSetForLinkWhichIsNotStored() throws Exception
	{
		startup(2);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.removeIncidenceSet(handle);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
		shutdown();
	}

	@Test
	public void thereAreNotIncidenceLinks() throws Exception
	{
		startup(3);
		final HGPersistentHandle handle = new UUIDPersistentHandle();
		storage.store(handle, new byte[] {});
		storage.removeIncidenceSet(handle);
		final HGRandomAccessResult<HGPersistentHandle> afterRemoving = storage
				.getIncidenceResultSet(handle);
		assertFalse(afterRemoving.hasNext());
		afterRemoving.close();
		shutdown();
	}

}
