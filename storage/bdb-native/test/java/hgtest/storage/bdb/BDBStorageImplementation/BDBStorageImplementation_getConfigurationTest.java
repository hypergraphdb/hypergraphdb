package hgtest.storage.bdb.BDBStorageImplementation;

import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.EnvironmentConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * In this test only some properties of configuration is checked:
 * <ul>
 * <li>read only</li>
 * <li>transactional</li>
 * </ul>
 */
public class BDBStorageImplementation_getConfigurationTest extends
		BDBStorageImplementationTestBasis
{
	@Test
	public void checkDatabaseConfig() throws Exception
	{
		startup();
		final DatabaseConfig databaseConfig = storage.getConfiguration()
				.getDatabaseConfig();
		assertFalse(databaseConfig.getReadOnly());
		assertTrue(databaseConfig.getTransactional());
		shutdown();
	}

	@Test
	public void checkEnvironmentConfig() throws Exception
	{
		startup();
		final EnvironmentConfig environmentConfig = storage.getConfiguration()
				.getEnvironmentConfig();
		assertTrue(environmentConfig.getTransactional());
		shutdown();
	}
}
