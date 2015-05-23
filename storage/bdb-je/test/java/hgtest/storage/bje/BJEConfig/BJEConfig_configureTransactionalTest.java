package hgtest.storage.bje.BJEConfig;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * {@link org.hypergraphdb.storage.bje.BJEConfig#configureTransactional()}
 * method tunes up two private fields: {@code envConfig} and {@code dbConfig}.
 * These two fields are checked in the appropriate test cases
 *
 * @author Yuriy Sechko
 */
public class BJEConfig_configureTransactionalTest
{
	@Test
	public void checkEnvironmentConfig() throws Exception
	{
		// initialize expected environment config as inside in {@link
		// org.hypergraphdb.storage.bje.BJEConfig#configureTransactional()}
		final EnvironmentConfig expected = new EnvironmentConfig();
		expected.setReadOnly(false);
		expected.setAllowCreate(true);
		expected.setCachePercent(30);
		expected.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
				Long.toString(10000000 * 10l));
		expected.setConfigParam(
				EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
				Long.toString(1024 * 1024));
		expected.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE,
				Long.toString(1024 * 1024));
		expected.setTransactional(true);
		final Durability newDurability = new Durability(
				Durability.SyncPolicy.WRITE_NO_SYNC,
				Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE);
		expected.setDurability(newDurability);

		final BJEConfig bjeConfig = new BJEConfig();
		bjeConfig.configureTransactional();
		final EnvironmentConfig actual = bjeConfig.getEnvironmentConfig();

		// equals() is not overridden in EnvironmentConfig class. So not sure
		// about its behavior.
		// But toString is overridden. It is pretty enough for comparing objects
		// in the present test case.
		assertEquals(actual.toString(), expected.toString());
	}

	@Test
	public void checkDatabaseConfig() throws Exception
	{
		// initialize expected database config as inside in {@link
		// org.hypergraphdb.storage.bje.BJEConfig#configureTransactional()}
		final DatabaseConfig expected = new DatabaseConfig();
		expected.setAllowCreate(true);
		expected.setTransactional(true);

        final BJEConfig bjeConfig = new BJEConfig();
		bjeConfig.configureTransactional();
		final DatabaseConfig actual = bjeConfig.getDatabaseConfig();

		// see note above about comparing
		assertEquals(actual.toString(), expected.toString());
	}
}
