package org.hypergraphdb.storage.bdb;

import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockDetectMode;

public class BDBConfig
{
    public static final long DEFAULT_STORE_CACHE = 20*1024*1024; // 20MB
    public static final int  DEFAULT_NUMBER_OF_STORAGE_CACHES = 1;
        
    private EnvironmentConfig envConfig;
    private DatabaseConfig dbConfig;
    private boolean storageMVCC = true;
    
    private void resetDefaults()
    {
        envConfig.setAllowCreate(true);
        envConfig.setInitializeCache(true);  
        envConfig.setCacheSize(DEFAULT_STORE_CACHE);
        envConfig.setCacheCount(DEFAULT_NUMBER_OF_STORAGE_CACHES);
        envConfig.setErrorPrefix("BERKELEYDB");
        envConfig.setErrorStream(System.out);          
        
        dbConfig.setAllowCreate(true);
        dbConfig.setType(DatabaseType.BTREE);
    }
    
    public BDBConfig()
    {
        envConfig = new EnvironmentConfig();
        dbConfig = new DatabaseConfig();
        resetDefaults();
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
        envConfig.setInitializeLogging(true);
        envConfig.setTransactional(true);            
        if (!storageMVCC)
        {
            envConfig.setInitializeLocking(true);
            envConfig.setLockDetectMode(LockDetectMode.RANDOM);
            envConfig.setMaxLockers(2000);
            envConfig.setMaxLockObjects(20000);
            envConfig.setMaxLocks(20000);                
        }
        else
        {
            envConfig.setMultiversion(true);
            envConfig.setTxnSnapshot(true);
        }
        envConfig.setTxnWriteNoSync(true);
        envConfig.setCachePageSize(4*1024);
        long maxActive = envConfig.getCacheSize() / envConfig.getCachePageSize();
        envConfig.setTxnMaxActive((int)maxActive*10);                   
        envConfig.setRunRecovery(true);
        envConfig.setRegister(true);
        envConfig.setLogAutoRemove(true);        
//          envConfig.setMaxMutexes(10000);
//        envConfig.setRunFatalRecovery(true);
        dbConfig.setTransactional(true);            
        if (storageMVCC)
            dbConfig.setMultiversion(true);
    }
    
    public boolean isStorageMVCC()
    {
        return storageMVCC;
    }

    public void setStorageMVCC(boolean storageMVCC)
    {
        this.storageMVCC = storageMVCC;
    }    
}