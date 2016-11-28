package hgtest.storage.bje.BJEStorageImplementation;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;

/**
 * In this test only some properties of configuration are checked:
 * <ul>
 * <li>read only</li>
 * <li>transactional</li>
 * </ul>
 */
public class BJEStorageImplementation_getConfigurationTest extends
		BJEStorageImplementationTestBasis
{
	@Test
	public void checkDatabaseConfig() throws Exception
	{
		final DatabaseConfig databaseConfig = storage.getConfiguration()
				.getDatabaseConfig();

		assertFalse(databaseConfig.getReadOnly());
		assertTrue(databaseConfig.getTransactional());
	}

	@Test
	public void checkEnvironmentConfig() throws Exception
	{
		final EnvironmentConfig environmentConfig = storage.getConfiguration()
				.getEnvironmentConfig();

		assertFalse(environmentConfig.getReadOnly());
		assertTrue(environmentConfig.getTransactional());
	}

	@Before
	public void startup() throws Exception
	{
		super.startup();
	}

	@After
	public void shutdown() throws Exception
	{
		super.shutdown();
	}
}
