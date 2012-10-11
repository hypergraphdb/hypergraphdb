package org.hypergraphdb.storage.bje;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;

public class BJEConfig
{
	// public static final long DEFAULT_STORE_CACHE = 20*1024*1024; // 20MB
	public static final long DEFAULT_STORE_CACHE = 100 * 1024 * 1024; // 100MB
																		// Alain
																		// for
																		// tests
	public static final int DEFAULT_NUMBER_OF_STORAGE_CACHES = 1;

	private EnvironmentConfig envConfig;
	private DatabaseConfig dbConfig;

	private void resetDefaults(boolean readOnly)
	{
		envConfig.setReadOnly(readOnly);
		dbConfig.setReadOnly(readOnly);

		envConfig.setAllowCreate(!readOnly);
		dbConfig.setAllowCreate(!readOnly);

		// envConfig.setCacheSize(DEFAULT_STORE_CACHE);
		envConfig.setCachePercent(30);

		envConfig.setConfigParam(EnvironmentConfig.LOG_FILE_MAX,
				Long.toString(10000000 * 10l));
		envConfig.setConfigParam(
				EnvironmentConfig.CLEANER_LOOK_AHEAD_CACHE_SIZE,
				Long.toString(1024 * 1024));
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_READ_SIZE,
				Long.toString(1024 * 1024));
	}

	public BJEConfig()
	{
		envConfig = new EnvironmentConfig();
		dbConfig = new DatabaseConfig();
		resetDefaults(false);
	}

	public EnvironmentConfig getEnvironmentConfig()
	{
		return envConfig;
	}

	public DatabaseConfig getDatabaseConfig()
	{
		return dbConfig;
	}

	public void configureTransactional()
	{
		envConfig.setTransactional(true);
		dbConfig.setTransactional(true);

		// envConfig.setLockDetectMode(LockDetectMode.RANDOM);

		Durability defaultDurability = new Durability(
				Durability.SyncPolicy.WRITE_NO_SYNC,
				Durability.SyncPolicy.NO_SYNC, // unused by non-HA applications.
				Durability.ReplicaAckPolicy.NONE); // unused by non-HA
													// applications.
		envConfig.setDurability(defaultDurability);
	}
}