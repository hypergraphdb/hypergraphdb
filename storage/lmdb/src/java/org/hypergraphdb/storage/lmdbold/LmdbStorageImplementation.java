package org.hypergraphdb.storage.lmdb;

import static org.fusesource.lmdbjni.Constants.NODUPDATA;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.CursorOp;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.DatabaseConfig;
import org.fusesource.lmdbjni.DatabaseEntry;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.Env;
import org.fusesource.lmdbjni.EnvConfig;
import org.fusesource.lmdbjni.JNI.MDB_envinfo;
import org.fusesource.lmdbjni.JNI.MDB_stat;
import org.fusesource.lmdbjni.LMDBException;
import org.fusesource.lmdbjni.Transaction;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.handle.LongHandleFactory;
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

public class LmdbStorageImplementation implements HGStoreImplementation
{
	private static final String DATA_DB_NAME = "datadb";
	private static final String PRIMITIVE_DB_NAME = "primitivedb";
	private static final String INCIDENCE_DB_NAME = "incidencedb";

	private LmdbConfig configuration;
	private HGStore store;
	private HGHandleFactory handleFactory;
	private Env env = null;
	private Database data_db = null;
	private Database primitive_db = null;
	private Database incidence_db = null;
	private HashMap<String, HGIndex<?, ?>> openIndices = new HashMap<String, HGIndex<?, ?>>();
	private ReentrantReadWriteLock indicesLock = new ReentrantReadWriteLock();
	private LinkBinding linkBinding = null;
	private final AtomicLong commitCount = new AtomicLong(0);

	private TransactionLmdbImpl txn()
	{
		HGTransaction tx = store.getTransactionManager().getContext().getCurrent();
		if (tx == null || tx.getStorageTransaction() instanceof VanillaTransaction)
			return TransactionLmdbImpl.nullTransaction();
		else
			return (TransactionLmdbImpl) tx.getStorageTransaction();
	}

	public LmdbStorageImplementation()
	{
		configuration = new LmdbConfig();
	}

	@Override
	public LmdbConfig getConfiguration()
	{
		return configuration;
	}

	public Env getEnvironment()
	{
		return env;
	}

	public long getNextCommitCount()
	{
		return commitCount.incrementAndGet();
	}

	@Override
	public void startup(HGStore store, HGConfiguration config)
	{
		this.store = store;
		this.handleFactory = config.getHandleFactory();
		this.linkBinding = new LinkBinding(store, handleFactory);
		EnvConfig envConfig = configuration.getEnvironmentConfig();
		File envDir = new File(store.getDatabaseLocation());
		envDir.mkdirs();

		try
		{
			//NativeUtils.loadLibraryFromJar("/native/liblmdbjni.so");
			//System.load("/hypergraphdb/storage/lmdb/target/classes/native/liblmdb.so");
//			System.loadLibrary("liblmdbjni.so");
			env = new Env();
			envConfig.setWriteMap(true);
			env.open(store.getDatabaseLocation(), envConfig);
			DatabaseConfig dbConfig = configuration.getDatabaseConfig().cloneConfig();
			data_db = env.openDatabase(DATA_DB_NAME, dbConfig);
			primitive_db = env.openDatabase(PRIMITIVE_DB_NAME, dbConfig);

			DatabaseConfig incConfig = configuration.getDatabaseConfig().cloneConfig();
			incConfig.setDupSort(true);
			incidence_db = env.openDatabase(INCIDENCE_DB_NAME, incConfig);

			openIndices = new HashMap<String, HGIndex<?, ?>>(); // force reset
																// since startup
																// can follow a
																// shutdown on
																// same opened
																// class
			checkPointThread = new CheckPointThread();
			checkPointThread.start();
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to initialize HyperGraph data store: " + ex.toString(), ex);
		}
	}

	@Override
	public void shutdown()
	{
		if (checkPointThread != null)
		{
			checkPointThread.stop = true;
			checkPointThread.interrupt();
			while (checkPointThread.running)
				try
				{
					Thread.sleep(500);
				}
				catch (InterruptedException ex)
				{
					/* need to wait here until it stops... */}
		}

		if (env != null)
		{
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

	@Override
	public void removeLink(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException("HGStore.remove called with a null handle.");
		try
		{
			data_db.delete(txn().getDbTransaction(), handle.toByteArray());
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove value with handle " + handle + ": " + ex.toString(), ex);
		}
	}

	@Override
	public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
	{
		try
		{
			// System.out.println("Adding, key:" +
			// ctString.hexDump(handle.toByteArray()) + ",data:" +
			// ctString.hexDump(data));
			byte[] key = handle.toByteArray();
			primitive_db.put(txn().getDbTransaction(), key, data);
			// System.out.println("PrimitivePut." + key.length + "," +
			// data.length);

			if (handleFactory instanceof LongHandleFactory)
			{
				txn().setLastId(((LongHandleFactory) handleFactory).getNext());
			}
			// if (result != OperationStatus.SUCCESS)
			// throw new Exception("OperationStatus: " + result);
			return handle;
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to store hypergraph raw byte []: " + data, ex);
		}
	}

	public HGPersistentHandle storeNextId(Transaction tx, HGPersistentHandle handle, byte[] data)
	{
		try
		{
			// System.out.println("Adding, key:" +
			// ctString.hexDump(handle.toByteArray()) + ",data:" +
			// ctString.hexDump(data));
			byte[] key = handle.toByteArray();
			primitive_db.put(tx, key, data);
			// System.out.println("PrimitivePut." + key.length + "," +
			// data.length);

			// if (result != OperationStatus.SUCCESS)
			// throw new Exception("OperationStatus: " + result);
			return handle;
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to store hypergraph raw byte []: " + data, ex);
		}
	}

	@Override
	public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
	{
		byte[] key = handle.toByteArray();
		DatabaseEntry value = new DatabaseEntry();
		linkBinding.objectToEntry(link, value);

		try
		{
			data_db.put(txn().getDbTransaction(), key, value.getData());
			// System.out.println("DataPut." + key.length + "," +
			// value.getData().length);

			if (handleFactory instanceof LongHandleFactory)
			{
				txn().setLastId(((LongHandleFactory) handleFactory).getNext());
			}
			// if (result != OperationStatus.SUCCESS)
			// throw new Exception("OperationStatus: " + result);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to store hypergraph link: " + ex.toString(), ex);
		}
		return handle;
	}

	@Override
	public void addIncidenceLink(HGPersistentHandle targetHandle, HGPersistentHandle linkHandle)
	{
		Cursor cursor = null;
		try
		{
			byte[] key = targetHandle.toByteArray();
			byte[] value = linkHandle.toByteArray();
			incidence_db.put(txn().getDbTransaction(), key, value, NODUPDATA);
			// System.out.println("IncidencePut." + key.length + "," +
			// value.length);

			// if (result != OperationStatus.SUCCESS && result !=
			// OperationStatus.KEYEXIST)
			// throw new Exception("OperationStatus: " + result);

			// cursor = incidence_db.openCursor(txn().getDbTransaction(),
			// cursorConfig);
			// OperationStatus status = cursor.getSearchBoth(key, value,
			// LockMode.DEFAULT);
			// if (status == OperationStatus.NOTFOUND)
			// {
			// OperationStatus result =
			// incidence_db.put(txn().getDbBTransaction(), key, value);
			// if (result != OperationStatus.SUCCESS)
			// throw new Exception("OperationStatus: " + result);
			// }
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to update incidence set for handle " + targetHandle + ": " + ex.toString(), ex);
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

//	@Override
	public void addIncidenceLinks(HGPersistentHandle linkHandle, Set<HGPersistentHandle> newTargets)
	{
		if (newTargets.size() <= 0)
		{
			return;
		}
		try
		{
			byte[] value = linkHandle.toByteArray();

			for (HGPersistentHandle newTarget : newTargets)
			{
				byte[] key = newTarget.toByteArray();
				byte[] orgValue = incidence_db.put(txn().getDbTransaction(), key, value, NODUPDATA);
				// System.out.println("IncidencePut." + key.length + ","
				// + value.length);

				// if (result != OperationStatus.SUCCESS && result !=
				// OperationStatus.KEYEXIST)
				// throw new Exception("OperationStatus: " + result);
			}
		} // incidence_db.stat(txn().getDbTransaction())
		catch (Exception ex)
		{
			throw new HGException("Failed to update incidence set for handle " + linkHandle + ": " + ex.toString(), ex);
		}
	}

	@Override
	public boolean containsLink(HGPersistentHandle handle)
	{
		byte[] key = handle.toByteArray();
		byte[] value;
		try
		{
			value = data_db.get(txn().getDbTransaction(), key);
			if (value != null)
			{
				// System.out.println(value.toString());
				return true;
			}
		}
		catch (LMDBException ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle + ": " + ex.toString(), ex);
		}

		return false;
	}

	@Override
	public boolean containsData(HGPersistentHandle handle)
	{
		byte[] key = handle.toByteArray();
		byte[] value;
		try
		{
			value = this.primitive_db.get(txn().getDbTransaction(), key);
			if (value != null)
			{
				// System.out.println(value.toString());
				return true;
			}
		}
		catch (LMDBException ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle + ": " + ex.toString(), ex);
		}

		return false;	}
	
	@Override
	public byte[] getData(HGPersistentHandle handle)
	{
		try
		{
			byte[] key = handle.toByteArray();
			return primitive_db.get(txn().getDbTransaction(), key);
		}
		catch (LMDBException ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle, ex);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException("HGStore.getIncidenceSet called with a null handle.");

		Cursor cursor = null;
		try
		{
			byte[] key = handle.toByteArray();
			Entry entry;
			TransactionLmdbImpl tx = txn();
			cursor = incidence_db.openCursor(tx.getDbTransaction());
			entry = cursor.get(CursorOp.SET, key);

			if (entry == null)
			{
				cursor.close();
				return (HGRandomAccessResult<HGPersistentHandle>) HGSearchResult.EMPTY;
			}
			else
				return new SingleKeyResultSet<HGPersistentHandle>(tx.attachCursor(cursor), new DatabaseEntry(entry.getKey()),
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

	@Override
	public long getIncidenceSetCardinality(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException("HGStore.getIncidenceSetCardinality called with a null handle.");

		Cursor cursor = null;
		try
		{
			byte[] key = handle.toByteArray();
			cursor = incidence_db.openCursor(txn().getDbTransaction());
			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry == null)
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

	@Override
	public HGPersistentHandle[] getLink(HGPersistentHandle handle)
	{
		try
		{
			byte[] key = handle.toByteArray();
			byte[] ba = data_db.get(txn().getDbTransaction(), key);
			if (ba != null)
			{
				DatabaseEntry value = new DatabaseEntry(ba);
				return linkBinding.entryToObject(value);
			}
			else
				return null;
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to retrieve link with handle " + handle, ex);
		}
	}

//	@Override
	public HGPersistentHandle getLastKeyPrimitive()
	{
		Cursor cursor = null;
		try
		{
			cursor = primitive_db.openCursor(txn().getDbTransaction());
			Entry entry = cursor.get(CursorOp.LAST);
			if (entry == null)
				return null;
			else
			{
				DatabaseEntry key = new DatabaseEntry(entry.getKey());
				return linkBinding.entryToObject(key)[0];
			}
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to retrieve last key for data db: " + ex.toString(), ex);
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

//	@Override
	public HGPersistentHandle getLastKeyDataDb()
	{
		Cursor cursor = null;
		try
		{
			cursor = data_db.openCursor(txn().getDbTransaction());
			Entry entry = cursor.get(CursorOp.LAST);
			if (entry == null)
				return null;
			else
			{
				DatabaseEntry key = new DatabaseEntry(entry.getKey());
				return linkBinding.entryToObject(key)[0];
			}
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to retrieve last key for data db: " + ex.toString(), ex);
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

	@Override
	public HGTransactionFactory getTransactionFactory()
	{
		return new HGTransactionFactory()
		{
			@Override
			public HGStorageTransaction createTransaction(HGTransactionContext context, HGTransactionConfig config,
					HGTransaction parent)
			{
				try
				{
					// System.out.println("LMDB create transact1 at:" +
					// System.currentTimeMillis());

					Transaction tx = null;
					if (parent != null)
					{
						assert !config.isReadonly() : "Can't create a child transaction on a read-only parent.";
						tx = env.createTransaction(((TransactionLmdbImpl) parent.getStorageTransaction()).getDbTransaction(),
								config.isReadonly());
					}
					else
						tx = env.createTransaction(config.isReadonly());
					// System.out.println("LMDB create transact2 at:" +
					// System.currentTimeMillis());

					return new TransactionLmdbImpl(LmdbStorageImplementation.this, handleFactory, tx, env, config.isReadonly());
				}
				catch (LMDBException ex)
				{
					// System.err.println("Failed to create transaction, will
					// exit - temporary behavior to be removed at some point.");
					ex.printStackTrace(System.err);
					// System.exit(-1);
					throw new HGException("Failed to create LMDB transaction object.", ex);
				}
			}

			@Override
			public boolean canRetryAfter(Throwable t)
			{
				return t instanceof TransactionConflictException;
			}
		};
	}

	@Override
	public void removeData(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException("HGStore.remove called with a null handle.");
		try
		{
			byte[] key = handle.toByteArray();
			// System.out.println("Deleting:" +
			// ctString.hexDump(key.getData()));
			primitive_db.delete(txn().getDbTransaction(), key);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove value with handle " + handle + ": " + ex.toString(), ex);
		}
	}

	@Override
	public void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink)
	{
		Cursor cursor = null;
		try
		{
			byte[] key = handle.toByteArray();
			byte[] value = oldLink.toByteArray();
			cursor = incidence_db.openCursor(txn().getDbTransaction());
			Entry entry = cursor.get(CursorOp.GET_BOTH, key, value);
			if (entry != null)
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
				try
				{
					cursor.close();
				}
				catch (Exception ex)
				{
				}
		}
	}

	@Override
	public void removeIncidenceSet(HGPersistentHandle handle)
	{
		try
		{
			byte[] key = handle.toByteArray();
			incidence_db.delete(txn().getDbTransaction(), key);
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
			return true;
		else
		{
			Database db = null;
			try
			{
				db = env.openDatabase(txn().getDbTransaction(), DefaultIndexImpl.DB_NAME_PREFIX + name, 0);
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
				return false;
		}
	}

//	@Override
//	public void compactDbs()
//	{
//		// Do nothing. Not applicable
//	}

	@Override
	@SuppressWarnings("unchecked")
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
			String name, 
			ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter, 
			Comparator<byte[]> keyComparator,
			Comparator<byte[]> valueComparator,
			boolean isBidirectional,
			boolean createIfNecessary)
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
				result = new DefaultBiIndexImpl<KeyType, ValueType>(name, this, store.getTransactionManager(), keyConverter,
						valueConverter,  keyComparator);
			else
				result = new DefaultIndexImpl<KeyType, ValueType>(name, this, store.getTransactionManager(), keyConverter,
						valueConverter, keyComparator);
			result.open();
			openIndices.put(name, result);
			return result;
		}
		finally
		{
			indicesLock.writeLock().unlock();
		}
	}

	@Override
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
				Database db = env.openDatabase(DefaultIndexImpl.DB_NAME_PREFIX + name);
				db.drop(true);
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
		boolean stop = false;
		boolean running = false;

		CheckPointThread()
		{
			this.setName("HGCHECKPOINT");
			this.setDaemon(true);
		}

		@Override
		public void run()
		{
			try
			{
				running = true;
				while (!stop)
				{
					Thread.sleep(60000);
					if (!stop)
						try
						{
							float percentFull = env.percentageFull();
							if (percentFull > 80.0f)
							{
								System.err.println(getStats());
								System.err.println("The database is getting too full. Now at " + percentFull + "%");
							}
						}
						catch (LMDBException ex)
						{
							throw new Error(ex);
						}
				}
			}
			catch (InterruptedException ex)
			{
				if (stop)
					try
					{
						float percentFull = env.percentageFull();
						if (percentFull > 80.0f)
						{
							System.err.println(getStats());
							System.err.println("The database is getting too full. Now at " + percentFull + "%");
						}
					}
					catch (LMDBException dx)
					{
						throw new Error(dx);
					}
				else
				{
					System.out.println(
							"Warning: HGDB CHECKPOINT THREAD got interrupted with: " + ex.toString() + ", stack trace follows...");
					ex.printStackTrace(System.out);
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

	/**
	 * Creates a data input object for reading a byte array of tuple data. A
	 * reference to the byte array will be kept by this object (it will not be
	 * copied) and therefore the byte array should not be modified while this
	 * object is in use.
	 * 
	 * @param buffer
	 *            is the byte array to be read and should contain data in tuple
	 *            format.
	 */
//	@Override
//	public HGDataInput newDataInput(byte[] buffer)
//	{
//		return LmdbDataInput.getInstance(buffer);
//	}

	/**
	 * Creates a data input object for reading a byte array of tuple data at a
	 * given offset for a given length. A reference to the byte array will be
	 * kept by this object (it will not be copied) and therefore the byte array
	 * should not be modified while this object is in use.
	 * 
	 * @param buffer
	 *            is the byte array to be read and should contain data in tuple
	 *            format.
	 * 
	 * @param offset
	 *            is the byte offset at which to begin reading.
	 * 
	 * @param length
	 *            is the number of bytes to be read.
	 */
//	@Override
//	public HGDataInput newDataInput(byte[] buffer, int offset, int length)
//	{
//		return LmdbDataInput.getInstance(buffer, offset, length);
//	}

	/**
	 * Creates a data input object from the data contained in a data output
	 * object. A reference to the data output's byte array will be kept by this
	 * object (it will not be copied) and therefore the data output object
	 * should not be modified while this object is in use.
	 * 
	 * @param output
	 *            is the data output object containing the data to be read.
	 */
//	@Override
//	public HGDataInput newDataInput(HGDataOutput output)
//	{
//		return LmdbDataInput.getInstance(output);
//	}

	/**
	 * Creates a data output object for writing a byte array of tuple data.
	 */
//	@Override
//	public HGDataOutput newDataOutput()
//	{
//		return LmdbDataOutput.getInstance();
//	}

	/**
	 * Creates a data output object for writing a byte array of tuple data,
	 * using a given buffer. A new buffer will be allocated only if the number
	 * of bytes needed is greater than the length of this buffer. A reference to
	 * the byte array will be kept by this object and therefore the byte array
	 * should not be modified while this object is in use.
	 *
	 * @param buffer
	 *            is the byte array to use as the buffer.
	 */
//	@Override
//	public HGDataOutput newDataOutput(byte[] buffer)
//	{
//		return LmdbDataOutput.getInstance(buffer);
//	}

//	@Override
	public String getStats()
	{
		StringBuilder sb = new StringBuilder();
		Env env = getEnvironment();

		sb.append("Env Version:");
		sb.append(Env.version());
		MDB_stat stat = env.stat();
		sb.append(" Stats:");
		sb.append(stat.toString());
		sb.append('\n');

		MDB_envinfo info = env.info();
		sb.append("Info:");
		sb.append(info.toString());
		sb.append('\n');

		MDB_envinfo info2 = env.info();

		Transaction dbTransaction = txn().getDbTransaction();
		if (dbTransaction != null)
		{
			stat = data_db.stat(dbTransaction);
			sb.append("Data Db Stats:");
			sb.append(stat.toString());
			sb.append('\n');

			stat = primitive_db.stat(dbTransaction);
			sb.append("Primitive Db Stats:");
			sb.append(stat.toString());
			sb.append('\n');

			stat = incidence_db.stat(dbTransaction);
			sb.append("Incidence Db Stats:");
			sb.append(stat.toString());
			sb.append('\n');

			for (Iterator<HGIndex<?, ?>> i = openIndices.values().iterator(); i.hasNext();)
			{
				sb.append(i.next().stats());
			}
		}

		float percentFull = env.percentageFull();
		sb.append("% Full:");
		sb.append(percentFull + "%");
		sb.append('\n');

		return sb.toString();
	}

	@Override
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name)
	{
		// TODO Auto-generated method stub
		return null;
	}
}
