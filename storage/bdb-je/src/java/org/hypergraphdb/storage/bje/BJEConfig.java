package org.hypergraphdb.storage.bje;

import java.util.concurrent.TimeUnit;

import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.Durability;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

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
		
		// The following is set tn  false for purely performance reasons, but the semantics
		// are terrible since transaction are not truly isolated anymore. The issue is
		// that BerkeleyDB Java Edition doesn't support proper snapshot isolation (MVCC), so
		// it works by setting lock timeouts and detecting conflicts this way. So when one
		// wants true isolation, one has to way for timeouts on conflicting transactions
		// and throughput is terrible. If we sacrifice isolation though, that leads to 
		// subtle non repeatable read issues (a record read twice in the same transaction giving
		// two different results because another transaction committed a new value).
		
		envConfig.setTxnSerializableIsolation(true);
		envConfig.setLockTimeout(100, TimeUnit.MILLISECONDS);

		Durability defaultDurability = new Durability(
				Durability.SyncPolicy.WRITE_NO_SYNC,
				Durability.SyncPolicy.NO_SYNC, // unused by non-HA applications.
				Durability.ReplicaAckPolicy.NONE); // unused by non-HA
													// applications.
		envConfig.setDurability(defaultDurability);
	}
}