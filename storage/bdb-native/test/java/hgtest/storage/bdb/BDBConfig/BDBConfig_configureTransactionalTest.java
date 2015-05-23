package hgtest.storage.bdb.BDBConfig;

import com.sleepycat.db.EnvironmentConfig;
import org.hypergraphdb.storage.bdb.BDBConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertNotNull;

/**
 * @author Yuriy Sechko
 */
public class BDBConfig_configureTransactionalTest
{
	@Test
	public void testNotNull() throws Exception
	{
		final BDBConfig bdbConfig = new BDBConfig();
		bdbConfig.configureTransactional();
		final EnvironmentConfig actual = bdbConfig.getEnvironmentConfig();

		// equals() is not overridden in EnvironmentConfig class.
		// And toString() is not overridden. There is not idea how to compare
		// two instances of EnvironmentConfig. So only check that retrieved
		// instance is not null.
		assertNotNull(actual);
	}
}
