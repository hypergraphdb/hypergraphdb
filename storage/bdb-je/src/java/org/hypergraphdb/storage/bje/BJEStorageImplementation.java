package org.hypergraphdb.storage.bje;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGStoreImplementation;
import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.transaction.HGTransactionContext;
import org.hypergraphdb.transaction.HGTransactionFactory;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.hypergraphdb.transaction.VanillaTransaction;
import org.hypergraphdb.util.HGClassLoaderDelegate;

import com.sleepycat.je.CheckpointConfig;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockConflictException;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import com.sleepycat.je.TransactionConfig;

public class BJEStorageImplementation implements HGStoreImplementation
{
	private static final String DATA_DB_NAME = "datadb";
	private static final String PRIMITIVE_DB_NAME = "primitivedb";
	private static final String INCIDENCE_DB_NAME = "incidencedb";

	private BJEConfig configuration;
	private HGStore store;
	private HGHandleFactory handleFactory;
	private CursorConfig cursorConfig = new CursorConfig();
	private Environment env = null;
	private Database data_db = null;
	private Database primitive_db = null;
	private Database incidence_db = null;
	private HashMap<String, HGIndex<?, ?>> openIndices = new HashMap<String, HGIndex<?, ?>>();
	private ReentrantReadWriteLock indicesLock = new ReentrantReadWriteLock();
	private LinkBinding linkBinding = null;

	private TransactionBJEImpl txn()
	{
		HGTransaction tx = store.getTransactionManager().getContext().getCurrent();

		if (tx == null || tx.getStorageTransaction() instanceof VanillaTransaction)
			return TransactionBJEImpl.nullTransaction();
		else
			return (TransactionBJEImpl) tx.getStorageTransaction();
	}

	public BJEStorageImplementation()
	{
		configuration = new BJEConfig();
	}

	public BJEConfig getConfiguration()
	{
		return configuration;
	}

	public Environment getBerkleyEnvironment()
	{
		return env;
	}

	public void startup(HGStore store, HGConfiguration config)
	{
		this.store = store;
		this.handleFactory = config.getHandleFactory();
		this.linkBinding = new LinkBinding(handleFactory);
		EnvironmentConfig envConfig = configuration.getEnvironmentConfig();
		envConfig.setConfigParam(EnvironmentConfig.CLEANER_THREADS, "5");
		envConfig.setClassLoader(new HGClassLoaderDelegate(config));
		if (config.isTransactional())
		{
			configuration.configureTransactional();
		}

		File envDir = new File(store.getDatabaseLocation());
		envDir.mkdirs();

		try
		{
			env = new Environment(envDir, envConfig);
			data_db = env.openDatabase(null, DATA_DB_NAME, configuration.getDatabaseConfig().clone());
			primitive_db = env.openDatabase(null, PRIMITIVE_DB_NAME, configuration.getDatabaseConfig().clone());

			DatabaseConfig incConfig = configuration.getDatabaseConfig().clone();
			incConfig.setSortedDuplicates(true);
			incidence_db = env.openDatabase(null, INCIDENCE_DB_NAME, incConfig);

			openIndices = new HashMap<String, HGIndex<?, ?>>(); // force reset
																// since startup
																// can follow a
																// shutdown on
																// same opened
																// class

			if (config.isTransactional())
			{
				CheckpointConfig ckptConfig = new CheckpointConfig();
				// System.out.println("checkpoint kbytes:" +
				// ckptConfig.getKBytes());
				// System.out.println("checkpoint minutes:" +
				// ckptConfig.getMinutes());
				env.checkpoint(null);
				checkPointThread = new CheckPointThread();
				checkPointThread.start();
			}
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to initialize HyperGraph data store: " + ex.toString(), ex);
		}
	}

	public void shutdown()
	{
		if (checkPointThread != null)
		{
			checkPointThread.stop = true;
			checkPointThread.interrupt();

			while (checkPointThread.running)
			{
				try
				{
					Thread.sleep(500);
				}
				catch (InterruptedException ex)
				{ /* need to wait here until it stops... */
				}
			}
		}

		if (env != null)
		{
			try
			{
				if (env.getConfig().getTransactional())
					env.checkpoint(null);
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}

			//
			// Close all indices
			//
			for (Iterator<HGIndex<?, ?>> i = openIndices.values().iterator(); i.hasNext();)
				try
				{
					i.next().close();
				}
				catch (Throwable t)
				{
					// TODO - we need to log the exception here, once we've
					// decided
					// on a logging mechanism.
					t.printStackTrace();
				}
			try
			{
				data_db.close();
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}

			try
			{
				primitive_db.close();
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}

			try
			{
				incidence_db.close();
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}

			try
			{
				env.close();
			}
			catch (Throwable t)
			{
				t.printStackTrace();
			}
		}
	}

	public void removeLink(HGPersistentHandle handle)
	{
		if (handle == null)
		{
			throw new NullPointerException("HGStore.remove called with a null handle.");
		}

		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			data_db.delete(txn().getBJETransaction(), key);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove value with handle " + handle + ": " + ex.toString(), ex);
		}
	}

	public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
	{
		try
		{
			OperationStatus result = primitive_db.put(txn().getBJETransaction(), new DatabaseEntry(handle.toByteArray()),
					new DatabaseEntry(data));
			if (result != OperationStatus.SUCCESS)
				throw new Exception("OperationStatus: " + result);
			return handle;
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to store hypergraph raw byte []: " + ex.toString(), ex);
		}
	}

	// public void store(MultipleEntry mkde, MultipleEntry mvde)
	// {
	// try
	// {
	// OperationStatus result =
	// primitive_db.putMultiple(txn().getBDBTransaction(),
	// mkde, mvde,
	// true);
	// if (result != OperationStatus.SUCCESS)
	// throw new Exception("OperationStatus: " + result);
	//
	// }
	// catch (Exception ex)
	// {
	// throw new HGException("Failed to store hypergraph MKDE : " +
	// ex.toString(), ex);
	// }
	// }

	public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
	{
		DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
		DatabaseEntry value = new DatabaseEntry();
		linkBinding.objectToEntry(link, value);

		try
		{
			OperationStatus result = data_db.put(txn().getBJETransaction(), key, value);
			if (result != OperationStatus.SUCCESS)
				throw new Exception("OperationStatus: " + result);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to store hypergraph link: " + ex.toString(), ex);
		}
		return handle;
	}

	public void addIncidenceLink(HGPersistentHandle handle, HGPersistentHandle newLink)
	{
		Cursor cursor = null;
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			DatabaseEntry value = new DatabaseEntry(newLink.toByteArray());
			OperationStatus result = incidence_db.putNoDupData(txn().getBJETransaction(), key, value);

			if (result != OperationStatus.SUCCESS && result != OperationStatus.KEYEXIST)
			{
				throw new Exception("OperationStatus: " + result);
			}

			// cursor = incidence_db.openCursor(txn().getBDBTransaction(),
			// cursorConfig);
			// OperationStatus status = cursor.getSearchBoth(key, value,
			// LockMode.DEFAULT);
			// if (status == OperationStatus.NOTFOUND)
			// {
			// OperationStatus result =
			// incidence_db.put(txn().getBDBTransaction(), key, value);
			// if (result != OperationStatus.SUCCESS)
			// throw new Exception("OperationStatus: " + result);
			// }
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to update incidence set for handle " + handle + ": " + ex.toString(), ex);
		}
		finally
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Exception ex)
				{
				}
		}
	}

	public boolean containsLink(HGPersistentHandle handle)
	{
		DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
		DatabaseEntry value = new DatabaseEntry();
		value.setPartial(0, 0, true);
		try
		{
			if (data_db.get(txn().getBJETransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
			{
				// System.out.println(value.toString());
				return true;
			}
		}
		catch (DatabaseException ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle + ": " + ex.toString(), ex);
		}

		return false;
	}

	public boolean containsData(HGPersistentHandle handle)
	{
		DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
		DatabaseEntry value = new DatabaseEntry();
		value.setPartial(0, 0, true);
		try
		{
			if (primitive_db.get(txn().getBJETransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
			{
				return true;
			}
		}
		catch (DatabaseException ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle + ": " + ex.toString(), ex);
		}
		return false;
	}

	public byte[] getData(HGPersistentHandle handle)
	{
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			if (primitive_db.get(txn().getBJETransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
				return value.getData();
			else
				return null;
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle, ex);
		}
	}

	@SuppressWarnings("unchecked")
	public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException("HGStore.getIncidenceSet called with a null handle.");

		Cursor cursor = null;
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			TransactionBJEImpl tx = txn();
			cursor = incidence_db.openCursor(tx.getBJETransaction(), cursorConfig);
			OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
			if (status == OperationStatus.NOTFOUND)
			{
				cursor.close();
				return (HGRandomAccessResult<HGPersistentHandle>) HGSearchResult.EMPTY;
			}
			else
				return new SingleKeyResultSet<HGPersistentHandle>(tx.attachCursor(cursor), key,
						BAtoHandle.getInstance(handleFactory));
		}
		catch (Throwable ex)
		{
			if (cursor != null)
				try
				{
					cursor.close();
				}
				catch (Throwable t)
				{
				}
			throw new HGException("Failed to retrieve incidence set for handle " + handle + ": " + ex.toString(), ex);
		}
	}

	public long getIncidenceSetCardinality(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException("HGStore.getIncidenceSetCardinality called with a null handle.");

		Cursor cursor = null;
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			cursor = incidence_db.openCursor(txn().getBJETransaction(), cursorConfig);
			OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
			if (status == OperationStatus.NOTFOUND)
				return 0;
			else
				return cursor.count();
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to retrieve incidence set for handle " + handle + ": " + ex.toString(), ex);
		}
		finally
		{
			try
			{
				cursor.close();
			}
			catch (Throwable t)
			{
			}
		}
	}

	public HGPersistentHandle[] getLink(HGPersistentHandle handle)
	{
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			DatabaseEntry value = new DatabaseEntry();
			if (data_db.get(txn().getBJETransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)
				return (HGPersistentHandle[]) linkBinding.entryToObject(value);
			else
				return null;
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle, ex);
		}
	}

	public HGTransactionFactory getTransactionFactory()
	{
		return new HGTransactionFactory()
		{
			public HGStorageTransaction createTransaction(HGTransactionContext context, HGTransactionConfig config,
					HGTransaction parent)
			{
				try
				{
					TransactionConfig tconfig = new TransactionConfig();

					Durability tDurability = new Durability(Durability.SyncPolicy.WRITE_NO_SYNC, 
															Durability.SyncPolicy.NO_SYNC, 
															Durability.ReplicaAckPolicy.NONE);
					tconfig.setDurability(tDurability);

					Transaction tx = null;

					if (parent != null)
					{
						// Nested transaction are not supported by JE Berkeley
						// DB.
						throw new IllegalStateException("Nested transaction detected. Not supported by JE Berkeley DB.");
						// tx =
						// env.beginTransaction(((TransactionBJEImpl)parent.getStorageTransaction()).getBJETransaction(),
						// tconfig);
						// tx = env.beginTransaction(null, tconfig);
					}
					else
					{
						tx = env.beginTransaction(null, tconfig);
					}
					return new TransactionBJEImpl(tx, env);
				}
				catch (DatabaseException ex)
				{
					// System.err.println("Failed to create transaction, will exit - temporary behavior to be removed at some point.");
					ex.printStackTrace(System.err);
					// System.exit(-1);
					throw new HGException("Failed to create BerkeleyDB transaction object.", ex);
				}
			}

			public boolean canRetryAfter(Throwable ex)
			{
				return ex instanceof TransactionConflictException || ex instanceof LockConflictException; // DeadlockException;

			}
		};
	}

	public void removeData(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException("HGStore.remove called with a null handle.");
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			primitive_db.delete(txn().getBJETransaction(), key);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove value with handle " + handle + ": " + ex.toString(), ex);
		}
	}

	public void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink)
	{
		Cursor cursor = null;
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			DatabaseEntry value = new DatabaseEntry(oldLink.toByteArray());
			cursor = incidence_db.openCursor(txn().getBJETransaction(), cursorConfig);
			OperationStatus status = cursor.getSearchBoth(key, value, LockMode.DEFAULT);
			if (status == OperationStatus.SUCCESS)
			{
				cursor.delete();
			}
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to update incidence set for handle " + handle + ": " + ex.toString(), ex);
		}
		finally
		{
			if (cursor != null)
			{
				try
				{
					cursor.close();
				}
				catch (Exception ex)
				{
				}
			}
		}
	}

	public void removeIncidenceSet(HGPersistentHandle handle)
	{
		try
		{
			DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
			incidence_db.delete(txn().getBJETransaction(), key);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove incidence set of handle " + handle + ": " + ex.toString(), ex);
		}
	}

	// ------------------------------------------------------------------------
	// INDEXING
	// ------------------------------------------------------------------------

	boolean checkIndexExisting(String name)
	{
		if (openIndices.get(name) != null)
		{
			return true;
		}
		else
		{
			DatabaseConfig cfg = new DatabaseConfig();
			cfg.setAllowCreate(false);
			Database db = null;

			try
			{
				db = env.openDatabase(null, DefaultIndexImpl.DB_NAME_PREFIX + name, cfg);
			}
			catch (Exception ex)
			{
			}

			if (db != null)
			{
				try
				{
					db.close();
				}
				catch (Throwable t)
				{
					t.printStackTrace();
				}
				return true;
			}
			else
			{
				return false;
			}
		}
	}

	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name) 
    {
		indicesLock.readLock().lock();		
		try 
        {
			return (HGIndex<KeyType, ValueType>)openIndices.get(name);
		}
		finally 
        {
			indicesLock.readLock().unlock();
		}
	}


	@SuppressWarnings("unchecked")
	@Override
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name, ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter, Comparator<byte[]> keyComparator, Comparator<byte[]> valueComparator, 
			boolean isBidirectional, boolean createIfNecessary)
	{
		indicesLock.readLock().lock();

		try
		{
			HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>) openIndices.get(name);
			if (idx != null)
				return idx;
			if (!checkIndexExisting(name) && !createIfNecessary)
				return null;
		}
		finally
		{
			indicesLock.readLock().unlock();
		}

		indicesLock.writeLock().lock();

		try
		{
			HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>) openIndices.get(name);
			if (idx != null)
				return idx;
			if (!checkIndexExisting(name) && !createIfNecessary)
				return null;

			DefaultIndexImpl<KeyType, ValueType> result = null;

			if (isBidirectional)
			{
				result = new DefaultBiIndexImpl<KeyType, ValueType>(name, this, store.getTransactionManager(), keyConverter,
						valueConverter, keyComparator, valueComparator);
			}
			else
			{
				result = new DefaultIndexImpl<KeyType, ValueType>(name, this, store.getTransactionManager(), keyConverter,
						valueConverter, keyComparator, valueComparator);
			}

			result.open();
			openIndices.put(name, result);
			return result;
		}
		finally
		{
			indicesLock.writeLock().unlock();
		}
	}

	public void removeIndex(String name)
	{
		indicesLock.writeLock().lock();

		try
		{
			HGIndex<?, ?> idx = openIndices.get(name);

			if (idx != null)
			{
				idx.close();
				openIndices.remove(name);
			}

			try
			{
				env.removeDatabase(null, DefaultIndexImpl.DB_NAME_PREFIX + name);
			}
			catch (Exception e)
			{
				throw new HGException(e);
			}
		}
		finally
		{
			indicesLock.writeLock().unlock();
		}
	}

	CheckPointThread checkPointThread = null;

	class CheckPointThread extends Thread
	{
		volatile boolean stop = false;
		volatile boolean running = false;

		CheckPointThread()
		{
			this.setName("HGCHECKPOINT");
			this.setDaemon(true);
		}

		public void run()
		{
			try
			{
				running = true;

				while (!stop)
				{
					Thread.sleep(60000);

					if (!stop)
					{
						try
						{
							env.checkpoint(null);
						}
						catch (DatabaseException ex)
						{
							throw new Error(ex);
						}
					}
				}
			}
			catch (InterruptedException ex)
			{
				if (stop)
				{
					try
					{
						env.checkpoint(null);
					}
					catch (DatabaseException dx)
					{
						throw new Error(dx);
					}
				}
			}
			catch (Throwable t)
			{
				System.err.println("HGDB CHECKPOINT THREAD exiting with: " + t.toString() + ", stack trace follows...");
				t.printStackTrace(System.err);
			}
			finally
			{
				running = false;
			}
		}
	}
}
