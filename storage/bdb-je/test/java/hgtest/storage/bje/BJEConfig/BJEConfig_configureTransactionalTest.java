package hgtest.storage.bje.BJEConfig;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import org.hypergraphdb.storage.bje.BJEConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

/**
 * Created 09.01.2015.
 */
public class BJEConfig_configureTransactionalTest
{
	@Test
	public void checkEnvironmentConfig() throws Exception
	{
		final BJEConfig bjeConfig = new BJEConfig();
		bjeConfig.configureTransactional();

		final EnvironmentConfig expected = new EnvironmentConfig();
		expected.setReadOnly(false);
		expected.setAllowCreate(true);
        expected.setCachePercent(30);
		expected.setConfigParam(
                EnvironmentConfig.LOG_FILE_MAX, Long.toString(10000000 * 10l));
		expected.setConfigParam(
                EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
                Long.toString(1024 * 1024));
		expected
				.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE,
                        Long.toString(1024 * 1024));
		expected.setTransactional(true);
		final Durability newDurability = new Durability(
				Durability.SyncPolicy.WRITE_NO_SYNC,
				Durability.SyncPolicy.NO_SYNC, Durability.ReplicaAckPolicy.NONE);
		expected.setDurability(newDurability);

		bjeConfig.configureTransactional();
		final EnvironmentConfig actual = bjeConfig
				.getEnvironmentConfig();
		assertEquals(actual.toString(), expected.toString());
	}

    @Test
    public void checkDatabaseConfig() throws Exception {
        final BJEConfig bjeConfig = new BJEConfig();

        final DatabaseConfig expected = new DatabaseConfig();
        expected.setAllowCreate(true);
        expected.setTransactional(true);

        bjeConfig.configureTransactional();
        final DatabaseConfig actual = bjeConfig.getDatabaseConfig();
        assertEquals(actual.toString(), expected.toString());
    }
}
