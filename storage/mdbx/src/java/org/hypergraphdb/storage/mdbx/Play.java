package org.hypergraphdb.storage.mdbx;

import java.io.File;
import java.util.Arrays;

import com.castortech.mdbxjni.Database;
import com.castortech.mdbxjni.DatabaseConfig;
import com.castortech.mdbxjni.Env;
import com.castortech.mdbxjni.EnvConfig;
import com.castortech.mdbxjni.Transaction;

public class Play
{
	
	static boolean cmp(byte[] A, byte[] B) {
		if (A == null)
			return B == null;
		else if (B == null || A.length != B.length)
			return false;
		else
			for (int i = 0; i < A.length; i++) {
				if (A[i] != B[i])
					return false;
			}
		return true;
	}

	static void checkData(Transaction tx, Database db, byte[] key, byte[] V) {
		try {
			byte[] data = db.get(tx, key);
			if (data == null)
				throw new RuntimeException("No data found for key.");
			else if (!cmp(data, V))
				throw new RuntimeException("Value different than expected.");
			else
				System.out.println("Data matches " + data[0] + ", " + data[1]);
		}
		catch (Exception ex) {
			throw new RuntimeException(ex);
		}
	}

	
	public static void main(String []argv)
	{
		System.out.println("bla");
		String databaseLocation = "/tmp/mdbx_test";

		EnvConfig envConfig = new EnvConfig();
		envConfig.setReadOnly(false);
		envConfig.setMaxDbs(15);
		envConfig.setMapSize(1024 * 1024 * 20);

		File envDir = new File(databaseLocation);
		envDir.mkdirs();

		Env env = new Env();
		try 
		{
			env.open(databaseLocation, envConfig);
			
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setCreate(true);
			Database db = env.openDatabase("testdb", dbConfig);

			byte[] key = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 };
			byte[] value0 = { 54, 23, 2 };

			Transaction tx = env.createTransaction();
			db.put(tx, key, value0);
			tx.commit();
			
			tx = env.createTransaction();

			checkData(tx, db, key, value0);
			
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}
		finally
		{
			env.close();
		}
	}
}
