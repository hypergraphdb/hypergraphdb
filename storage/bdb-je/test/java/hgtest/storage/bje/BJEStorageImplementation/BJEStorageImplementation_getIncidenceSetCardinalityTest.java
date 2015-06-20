package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;

/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_getIncidenceSetCardinalityTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void useNullHandle() throws Exception
	{
		final Exception expected = new NullPointerException(
				"HGStore.getIncidenceSetCardinality called with a null handle.");

		startup();
		try
		{
			storage.getIncidenceSetCardinality(null);
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
			final String expectedMessage = String
					.format("Failed to retrieve incidence set for handle %s: java.lang.IllegalArgumentException: Exception in test case.",
							handle);
			assertExceptions(ex, HGException.class, expectedMessage);
		}
		finally
		{
			shutdown();
		}
	}
}
