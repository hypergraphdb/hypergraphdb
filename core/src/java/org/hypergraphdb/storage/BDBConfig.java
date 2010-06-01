package org.hypergraphdb.storage;

import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.EnvironmentConfig;

public class BDBConfig
{
    private EnvironmentConfig envConfig;
    private DatabaseConfig dbConfig;
    
    public BDBConfig()
    {
        
    }
    
    public EnvironmentConfig getEnvironmentConfig()
    {
        return envConfig;
    }
    
    public DatabaseConfig getDatabaseConfig()
    {
        return dbConfig;
    }
}