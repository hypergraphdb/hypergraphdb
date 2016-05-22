package hgtest.storage.bje.BJEConfig;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static com.sleepycat.je.EnvironmentConfig.LOG_FILE_MAX;
import static com.sleepycat.je.EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE;
import static com.sleepycat.je.EnvironmentConfig.CLEANER_READ_SIZE;
import static com.sleepycat.je.Durability.SyncPolicy.WRITE_NO_SYNC;
import static com.sleepycat.je.Durability.SyncPolicy.NO_SYNC;
import static com.sleepycat.je.Durability.ReplicaAckPolicy.NONE;

/**
 * <p>
 * Method
 * {@link org.hypergraphdb.storage.bje.BJEConfig#configureTransactional()}
 * tested here tunes up two private fields: <code>envConfig</code> and
 * <code>dbConfig</code>. Test cases below ensure that these fields can be
 * assigned correctly.
 * </p>
 */
public class BJEConfig_configureTransactionalTest
{
	@Test
	public void checkEnvironmentConfig() throws Exception
	{
		final EnvironmentConfig expectedConfig = new EnvironmentConfig();
		expectedConfig
				.setReadOnly(false)
				.setAllowCreate(true)
				.setCachePercent(30)
				.setConfigParam(LOG_FILE_MAX, Long.toString(100_000_000L))
				.setConfigParam(CLEANER_LOOK_AHEAD_CACHE_SIZE,
						Long.toString(1024 * 1024))
				.setConfigParam(CLEANER_READ_SIZE, Long.toString(1024 * 1024));
		expectedConfig.setTransactional(true);
		expectedConfig.setDurability(new Durability(WRITE_NO_SYNC, NO_SYNC,
				NONE));

		final BJEConfig configUnderTest = new BJEConfig();
		configUnderTest.configureTransactional();
		final EnvironmentConfig actualConfig = configUnderTest
				.getEnvironmentConfig();

		assertEquals(expectedConfig.toString(), actualConfig.toString());
	}

	@Test
	public void checkDatabaseConfig() throws Exception
	{
		final DatabaseConfig expectedConfig = new DatabaseConfig()
				.setAllowCreate(true).setTransactional(true);

		final BJEConfig configUnderTest = new BJEConfig();
		configUnderTest.configureTransactional();
		final DatabaseConfig actualConfig = configUnderTest.getDatabaseConfig();

		assertEquals(expectedConfig.toString(), actualConfig.toString());
	}
}
