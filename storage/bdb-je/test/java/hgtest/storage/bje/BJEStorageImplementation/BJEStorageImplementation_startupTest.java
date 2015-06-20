package hgtest.storage.bje.BJEStorageImplementation;

import org.hypergraphdb.HGException;
import org.testng.annotations.Test;

import static hgtest.storage.bje.TestUtils.assertExceptions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_startupTest extends
		BJEStorageImplementationTestBasis
{

	@Test
	public void environmentIsTransactional() throws Exception
	{
		startup();
		final boolean isTransactional = storage.getConfiguration()
				.getDatabaseConfig().getTransactional();
		assertTrue(isTransactional);
		shutdown();
	}

	@Test
	public void storageIsNotReadOnly() throws Exception
	{
		startup();
		final boolean isReadOnly = storage.getConfiguration()
				.getDatabaseConfig().getReadOnly();
		assertFalse(isReadOnly);
		shutdown();
	}

	@Test
	public void checkDatabaseName() throws Exception
	{
		startup();
		final String databaseLocation = storage.getBerkleyEnvironment()
				.getHome().getPath();
		assertEquals(databaseLocation, testDatabaseLocation);
		shutdown();
	}

	@Test
	public void exceptionWhileStartupOccurred() throws Exception
	{
		final Exception expected = new HGException(
				"Failed to initialize HyperGraph data store: java.lang.IllegalStateException: Throw exception in test case.");

		try
		{
			startup(1, new IllegalStateException(
					"Throw exception in test case."));
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
	public void environmentIsNotTransactional() throws Exception
	{
		startupNonTransactional();
		shutdown();
	}
}
