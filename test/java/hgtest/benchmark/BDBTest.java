package hgtest.benchmark;

import java.io.File;
import java.util.UUID;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.TransactionConfig;

public class BDBTest
{
    public static void main(String [] argv)
    {
        String dbLocation = "/tmp/bdbtest";
        
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setInitializeCache(true);  
        envConfig.setErrorPrefix("BERKELEYDB");
        envConfig.setErrorStream(System.out);        
        envConfig.setInitializeLocking(true);
        envConfig.setInitializeLogging(true);
        envConfig.setTransactional(true);
        envConfig.setMultiversion(true);
        envConfig.setTxnSnapshot(true);
        envConfig.setTxnWriteNoSync(true);
        envConfig.setTxnMaxActive(5000);
        envConfig.setLockDetectMode(LockDetectMode.RANDOM);
        envConfig.setRunRecovery(true);
        envConfig.setRegister(true);
        envConfig.setLogAutoRemove(true);
        envConfig.setMaxLockers(2000);
        envConfig.setMaxLockObjects(20000);
        envConfig.setMaxLocks(20000);
        
        File envDir = new File(dbLocation);
        envDir.mkdirs();
        try
        {
            Environment env = new Environment(envDir, envConfig);
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            dbConfig.setTransactional(true);
            dbConfig.setType(DatabaseType.BTREE);
            Database db = env.openDatabase(null, "test", null, dbConfig);    
            
            long start = System.currentTimeMillis();
            for (int i = 0; i < 20000; i++)
            {
                Transaction tx = env.beginTransaction(null, new TransactionConfig());
                DatabaseEntry dbkey = new DatabaseEntry(UUID.randomUUID().toString().getBytes());
                DatabaseEntry dbvalue = new DatabaseEntry(UUID.randomUUID().toString().getBytes());                 
                db.put(tx, dbkey, dbvalue);
                tx.commit();
            }
            System.out.println("Done: " + (System.currentTimeMillis() - start));
            db.close();
            env.checkpoint(null);
            env.close();            
            System.out.println("Env closed/committed: " + (System.currentTimeMillis() - start));
        }
        catch (Exception ex)
        {
            ex.printStackTrace(System.err);
        }
    }
}
