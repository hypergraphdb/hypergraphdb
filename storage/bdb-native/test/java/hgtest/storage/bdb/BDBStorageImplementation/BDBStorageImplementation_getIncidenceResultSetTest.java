package hgtest.storage.bdb.BDBStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bdb.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
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
			assertExceptions(ex, HGException.class,
					"Failed to retrieve incidence set for",
					"java.lang.IllegalStateException: Exception in test case.");
		}
		finally
		{
			shutdown();
		}
	}
}
