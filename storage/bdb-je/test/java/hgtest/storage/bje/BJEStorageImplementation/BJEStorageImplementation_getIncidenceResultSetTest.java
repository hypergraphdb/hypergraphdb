package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;


/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_getIncidenceResultSetTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void useNullHandle() throws Exception
	{
		final Exception expected = new NullPointerException(
				"HGStore.getIncidenceSet called with a null handle.");

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
			final String expectedMessage = String
					.format("Failed to retrieve incidence set for handle %s: java.lang.IllegalStateException: Exception in test case.",
							handle);
			assertExceptions(ex, HGException.class, expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
