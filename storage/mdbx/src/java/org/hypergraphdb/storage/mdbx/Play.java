package org.hypergraphdb.storage.mdbx;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.util.HGUtils;
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

	
	public static void main_mdbx(String []argv)
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
	
	static void checkIncidence(HGPersistentHandle atom, HashSet<HGPersistentHandle> incidenceSet, HGStore store)
	{
		store.getTransactionManager().ensureTransaction(() -> {
			long storedIncidentCount = store.getIncidenceSetCardinality(atom);
				
			if (storedIncidentCount != incidenceSet.size())
				throw new RuntimeException("Not same number of incident links,  " + storedIncidentCount +
						", expecting " + incidenceSet.size());
			
			try (HGRandomAccessResult<HGPersistentHandle> rs = store.getIncidenceResultSet(atom))
			{
				while (rs.hasNext())
					if (!incidenceSet.contains(rs.next()))
						throw new RuntimeException("Did store incident link: " + rs.current());
					else
						System.out.println("INcident " + rs.current() + " is correct.");
			}				
			return null;
		});		
	}
	
	
	public static void main(String []argv)
	{
		File location = new File("/Users/borislav/code/hgdbtemp/data_mdbx");
		HGUtils.dropHyperGraphInstance(location.getAbsolutePath());
		location.mkdirs();
		HGConfiguration config = new HGConfiguration();
		MdbxStorageImplementation storageImpl = new MdbxStorageImplementation();
		config.setStoreImplementation(storageImpl);
		HGStore store = new HGStore(location.getAbsolutePath(), config);
		try
		{
			HGPersistentHandle h = config.getHandleFactory().makeHandle();
			
			store.getTransactionManager().ensureTransaction(() -> {
				storageImpl.store(h, "Hello world".getBytes());
				byte [] back = storageImpl.getData(h);
				System.out.println(new String(back));			
				return h;
			});
			
			storageImpl.shutdown();
			storageImpl.startup(store, config);

			store.getTransactionManager().ensureTransaction(() -> {
				byte [] back = storageImpl.getData(h);
				System.out.println(new String(back));
				return h;
			});			

			HGPersistentHandle [] linkData = new HGPersistentHandle[] {
					config.getHandleFactory().makeHandle(),
					config.getHandleFactory().makeHandle(),
					config.getHandleFactory().makeHandle()
			};
			HGPersistentHandle otherLink = config.getHandleFactory().makeHandle();
			HGPersistentHandle linkH = store.getTransactionManager().ensureTransaction(() -> {
				storageImpl.store(otherLink, new HGPersistentHandle[]{
					config.getHandleFactory().makeHandle(),
					config.getHandleFactory().makeHandle()
				});
				return storageImpl.store(config.getHandleFactory().makeHandle(), linkData);
			});			
			System.out.println("Links arrays are equal= " + HGUtils.eq(linkData, 
					store.getTransactionManager().ensureTransaction(() -> {
						return storageImpl.getLink(linkH);
					})));
			System.out.println("Links arrays are equal= " + HGUtils.eq(linkData,
					store.getTransactionManager().ensureTransaction(() -> {
						return storageImpl.getLink(otherLink);
					})));			

			HashSet<HGPersistentHandle> incidenceSet = new HashSet<HGPersistentHandle>();
			for (int i = 0; i < 11; i++)
				incidenceSet.add(config.getHandleFactory().makeHandle());
			
			store.getTransactionManager().ensureTransaction(() -> {
				for (HGPersistentHandle incident : incidenceSet)
					storageImpl.addIncidenceLink(linkH, incident);
				return null;
			});
			
			
			checkIncidence(linkH, incidenceSet, store);
			
			
			HGPersistentHandle removed = incidenceSet.stream().skip(4).findFirst().get();
			HGPersistentHandle anotherRemoved = incidenceSet.stream().skip(2).findFirst().get();
			incidenceSet.remove(removed);
			incidenceSet.remove(anotherRemoved);
			store.getTransactionManager().ensureTransaction(() -> {
				storageImpl.removeIncidenceLink(linkH, removed);
				storageImpl.removeIncidenceLink(linkH, anotherRemoved);
				return null;
			});
			
			checkIncidence(linkH, incidenceSet, store);
			
			store.getTransactionManager().ensureTransaction(() -> {
				storageImpl.removeIncidenceSet(linkH);
				return null;
			});
			
			if (store.getTransactionManager().ensureTransaction(() -> storageImpl.getIncidenceSetCardinality(linkH))
					!= 0)
				throw new RuntimeException("Incience set for " + linkH + " should be clear.");
			
			
			
		}
		catch (Throwable tx)
		{
			tx.printStackTrace();
		}
		finally
		{
			storageImpl.shutdown();	
		}		
	}
}
