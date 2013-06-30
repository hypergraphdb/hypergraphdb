package hgtest.storage.bje;

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertNull;

/**
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
	public void databaseNameAsSpecifiedInStore() throws Exception
	{
		startup();
		final String databaseLocation = storage.getBerkleyEnvironment()
				.getHome().getPath();
		assertEquals(databaseLocation, testDatabaseLocation);
		shutdown();
	}
}
