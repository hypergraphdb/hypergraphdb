package hgtest;

import java.io.File;

import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.Lock;
import com.sleepycat.db.LockDetectMode;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.LockRequestMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.Transaction;
import com.sleepycat.db.TransactionConfig;

public class BDBTx
{
	static boolean cmp(byte [] A, byte [] B)
	{
		if (A == null) return B == null;
		else if (B == null || A.length != B.length) return false;
		else for (int i = 0; i < A.length; i++)
		{
			if (A[i] != B[i]) return false;
		}
		return true;
	}
	
	static void checkData(Transaction tx, Database db, DatabaseEntry key, byte [] V)
	{
		DatabaseEntry data = new DatabaseEntry();
		try
		{
			if (db.get(tx, key, data, LockMode.DEFAULT) != OperationStatus.SUCCESS)
				throw new RuntimeException("No data found for key.");
			else if (!cmp(data.getData(), V))
				throw new RuntimeException("Value different than expected.");
		}
		catch (Exception ex)
		{
			throw new RuntimeException(ex);
		}
	}
	
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
/*            lockerId = env.createLockerID();
            byte [] lockObject = new byte[] { 1, 2, 3, 4, 5 };
            Lock lock = env.getLock(lockerId, false, new DatabaseEntry(lockObject), LockRequestMode.READ);
            System.out.println("Go lock " + lock); */
            
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            if (env.getConfig().getTransactional())
            	dbConfig.setTransactional(true);
            dbConfig.setType(DatabaseType.BTREE);            
            Database db = env.openDatabase(null, "testdb", null, dbConfig);
            
            DatabaseEntry key = new DatabaseEntry(new byte []{1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16});
            byte [] value0 = {0,1,2};
            
            Transaction tx = env.beginTransaction(null, new TransactionConfig());
            db.put(null, key, new DatabaseEntry(value0));
            tx.commit();
            
            tx = env.beginTransaction(null, new TransactionConfig());
            DatabaseEntry data = new DatabaseEntry(); 
            
            checkData(tx, db, key, value0);
            
            byte [] value1 = {23,24,23};            
            db.put(tx, key, new DatabaseEntry(value1));
            checkData(tx, db, key, value1);
            
            byte [] value2 = {12, 4, 56, 7};
            db.put(tx, key, new DatabaseEntry(value2));
            checkData(tx, db, key, value2);
            
            byte [] value3 = {3,2,3,45,56,};
            db.put(tx, key, new DatabaseEntry(value3));
            checkData(tx, db, key, value3);
            
            tx.commit();
            
            
            tx = env.beginTransaction(null, new TransactionConfig()); 
            
            checkData(tx, db, key, value3);
            
            value1 = new byte [] {23,24,23};            
            db.put(tx, key, new DatabaseEntry(value1));
            checkData(tx, db, key, value1);
            
            value2 = new byte [] {12, 4, 56, 7};
            db.put(tx, key, new DatabaseEntry(value2));
            checkData(tx, db, key, value2);
            
            value3 = new byte []{3,2,3,45,56,};
            db.put(tx, key, new DatabaseEntry(value3));
            checkData(tx, db, key, value3);
            
            tx.commit();
            
            db.close();
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