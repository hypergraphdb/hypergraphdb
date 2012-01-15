package hgtest;

import java.io.File;

import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.Lock;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.LockRequestMode;

public class BDBLock
{
	public static void main(String [] argv)
	{
        String databaseLocation = "c:/tmp/dblock";
        
        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setInitializeCache(true);  
        envConfig.setCacheSize(20*1014*1024);
        envConfig.setCacheCount(50);
        envConfig.setErrorPrefix("BERKELEYDB");
        envConfig.setErrorStream(System.out);        
        envConfig.setInitializeLocking(true);
        envConfig.setInitializeLogging(true);
        envConfig.setTransactional(true);
        envConfig.setTxnWriteNoSync(true);
        envConfig.setLockDetectMode(LockDetectMode.RANDOM);
        envConfig.setRunRecovery(true);
        envConfig.setRegister(true);
        envConfig.setLogAutoRemove(true);
        envConfig.setMaxLockers(200);
        envConfig.setMaxLockObjects(20000);
        envConfig.setMaxLocks(20000);
        
        File envDir = new File(databaseLocation);
        envDir.mkdirs();
        
        Environment env = null;
        int lockerId = 0;
        try
        {
            env = new Environment(envDir, envConfig);            
            lockerId = env.createLockerID();
            byte [] lockObject = new byte[] { 1, 2, 3, 4, 5 };
            Lock lock = env.getLock(lockerId, false, new DatabaseEntry(lockObject), LockRequestMode.READ);
            System.out.println("Go lock " + lock);
        }
        catch (Exception ex)
        {
        	ex.printStackTrace();
        }
        finally
        {
        	try 
        	{
        		if (lockerId != 0)
        			env.freeLockerID(lockerId);
        		env.close(); 
        	}
        	catch (Exception ex) { ex.printStackTrace(); }        	
        }
	}
}