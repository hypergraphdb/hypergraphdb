package hgtest.storage.bje.BJEStorageImplementation;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.EnvironmentConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * In this test only some properties of configuration is checked:
 * <ul>
 * <li>read only</li>
 * <li>transactional</li>
 * </ul>
 *
 * @author Yuriy Sechko
 */
public class BJEStorageImplementation_getConfigurationTest extends
		BJEStorageImplementationTestBasis
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

		assertFalse(environmentConfig.getReadOnly());
		assertTrue(environmentConfig.getTransactional());
		shutdown();
	}
}
