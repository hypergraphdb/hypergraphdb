package org.hypergraphdb.storage.mdbx;

import static com.castortech.mdbxjni.Constants.NODUPDATA;
import static com.google.common.base.Preconditions.checkState;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.castortech.mdbxjni.Cursor;
import com.castortech.mdbxjni.CursorOp;
import com.castortech.mdbxjni.Database;
import com.castortech.mdbxjni.DatabaseConfig;
import com.castortech.mdbxjni.DatabaseEntry;
import com.castortech.mdbxjni.Entry;
import com.castortech.mdbxjni.Env;
import com.castortech.mdbxjni.EnvConfig;
import com.castortech.mdbxjni.EnvInfo;
import com.castortech.mdbxjni.JNI.MDBX_stat;
//import com.castortech.util.ClassUtil;
//import com.castortech.util.ctString;
import com.castortech.mdbxjni.MDBXException;
import com.castortech.mdbxjni.Transaction;
//import org.hypergraphdb.HGCipherInfo;
import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGIndex;
//import org.hypergraphdb.HGIndexConfig;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGStore;
//import org.hypergraphdb.handle.SequenceHandleFactory;
import org.hypergraphdb.storage.BAtoBA;
import org.hypergraphdb.storage.BAtoHandle;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.HGStoreImplementation;
//import org.hypergraphdb.storage.encrypt.EncryptUtils;
//import org.hypergraphdb.storage.encrypt.IvProvider;
//import org.hypergraphdb.storage.encrypt.SecretProvider;
import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionConfig;
import org.hypergraphdb.transaction.HGTransactionContext;
import org.hypergraphdb.transaction.HGTransactionFactory;
import org.hypergraphdb.transaction.TransactionConflictException;
import org.hypergraphdb.transaction.VanillaTransaction;
//import org.hypergraphdb.type.HGDataInput;
//import org.hypergraphdb.type.HGDataOutput;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;

public class MdbxStorageImplementation implements HGStoreImplementation
{
//	private static final Logger log = LoggerFactory
//			.getLogger(MdbxStorageImplementation.class);

	private static final String DATA_DB_NAME = "datadb";
	private static final String PRIMITIVE_DB_NAME = "primitivedb";
	private static final String INCIDENCE_DB_NAME = "incidencedb";

	private static final String FAILED_RETRIEVE_LINK_WITH_HDL = "Failed to retrieve link with handle ";
	private static final String FAILED_UPDATE_INCIDENCE_SET_FOR_HDL = "Failed to update incidence set for handle ";

	private HGConfiguration hgConfig;
	private MdbxConfig configuration;
	private HGStore store;
	private HGHandleFactory handleFactory;
	private Env env = null;
	private Database dataDb = null;
	private Database primitiveDb = null;
	private Database incidenceDb = null;
	private Map<String, HGIndex<?, ?>> openIndices = new HashMap<>();
	private ReentrantReadWriteLock indicesLock = new ReentrantReadWriteLock();
	private LinkBinding linkBinding = null;
	private final AtomicLong commitCount = new AtomicLong(0);

//	private EncryptUtils encryptUtils;

	private TransactionMdbxImpl txn()
	{
		HGTransaction tx = store.getTransactionManager().getContext()
				.getCurrent();
		if (tx == null
				|| tx.getStorageTransaction() instanceof VanillaTransaction)
			return TransactionMdbxImpl.nullTransaction();
		else
			return (TransactionMdbxImpl) tx.getStorageTransaction();
	}

	public MdbxStorageImplementation(HGConfiguration hgConfig)
	{
		this.hgConfig = hgConfig;
		configuration = new MdbxConfig(hgConfig);
	}

	@Override
	public MdbxConfig getConfiguration()
	{
		return configuration;
	}

	public Env getEnvironment()
	{
		return env;
	}

//	/* package */EncryptUtils getEncryptUtils()
//	{
//		return encryptUtils;
//	}

	public long getNextCommitCount()
	{
		return commitCount.incrementAndGet();
	}

	@Override
	public void startup(HGStore store, HGConfiguration config)
	{
		this.store = store;
		handleFactory = config.getHandleFactory();
		linkBinding = new LinkBinding(this, handleFactory);

		EnvConfig envConfig = configuration.getEnvironmentConfig();
//		if (log.isInfoEnabled())
//		{
//			log.info(getConfiguration().toStringDetail());
//		}

		File envDir = new File(store.getDatabaseLocation());
		envDir.mkdirs();

		try
		{
//			IvProvider ivProvider = new IvProvider(new File(envDir, "ivs.dat"));
//			SecretProvider secretProvider = new SecretProvider(envDir);
//			encryptUtils = new EncryptUtils(secretProvider, ivProvider);

			env = new Env();
			Env.pushMemoryPool(1024 * 512);
			env.open(store.getDatabaseLocation(), envConfig);
			DatabaseConfig dbConfig = configuration.getDatabaseConfig().cloneConfig();
			dataDb = env.openDatabase(DATA_DB_NAME, dbConfig);
			primitiveDb = env.openDatabase(PRIMITIVE_DB_NAME, dbConfig);

			DatabaseConfig incConfig = configuration.getDatabaseConfig()
					.cloneConfig();
			incConfig.setDupSort(true);
			incidenceDb = env.openDatabase(INCIDENCE_DB_NAME, incConfig);

			openIndices = new HashMap<>(); // force reset since startup can
											// follow a shutdown on same opened
											// class
//			checkPointThread = new CheckPointThread();
//			checkPointThread.start();
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to initialize HyperGraph data store: "
					+ ex.toString(), ex);
		}
	}

	@Override
	public void shutdown()
	{
//		if (checkPointThread != null)
//		{
//			checkPointThread.stop = true;
//			checkPointThread.interrupt();
//			while (checkPointThread.running)
//				try
//				{
//					Thread.sleep(500);
//				}
//				catch (InterruptedException ex)
//				{
//					/* need to wait here until it stops... */}
//		}

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
				catch (Exception e)
				{
//					log.error("Error closing indices", e);
				}

			Env.popMemoryPool();

			try
			{
				dataDb.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}

			try
			{
				primitiveDb.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}

			try
			{
				incidenceDb.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}

			try
			{
				env.close();
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public void removeLink(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException(
					"HGStore.remove called with a null handle.");

		try
		{
			dataDb.delete(txn().getDbTransaction(), handle.toByteArray());
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove value with handle " + handle
					+ ": " + ex.toString(), ex);
		}
	}

	@Override
	public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
	{
		try
		{
//			if (log.isTraceEnabled())
//				log.trace("Adding, key:{},data:{}",
//						ctString.hexDump(handle.toByteArray()),
//						ctString.hexDump(data));
			byte[] key = handle.toByteArray();
			primitiveDb.put(txn().getDbTransaction(), key, data);
//			log.trace("PrimitivePut.{},{}", key.length, data.length);

//			if (handleFactory instanceof SequenceHandleFactory)
//			{
//				txn().setLastId(
//						((SequenceHandleFactory) handleFactory).getNext());
//			}
//		if (result != OperationStatus.SUCCESS)
//			throw new Exception("OperationStatus: " + result);
			return handle;
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to store hypergraph raw byte []: " + data, ex);
		}
	}

//	public HGPersistentHandle storeNextId(Transaction tx,
//			HGPersistentHandle handle, byte[] data)
//	{
//		try
//		{
//			if (log.isTraceEnabled())
//				log.trace("Adding, key:{},data:{}",
//						ctString.hexDump(handle.toByteArray()),
//						ctString.hexDump(data));
//			byte[] key = handle.toByteArray();
//			primitiveDb.put(tx, key, data);
//			log.trace("PrimitivePut.{},{}", key.length, data.length);
//
////		if (result != OperationStatus.SUCCESS)
////			throw new Exception("OperationStatus: " + result);
//			return handle;
//		}
//		catch (Exception ex)
//		{
//			throw new HGException(
//					"Failed to store hypergraph raw byte []: " + data, ex);
//		}
//	}

	@Override
	public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
	{
		byte[] key = handle.toByteArray();
		DatabaseEntry value = new DatabaseEntry();
		linkBinding.objectToEntry(link, value);

		try
		{
			dataDb.put(txn().getDbTransaction(), key, value.getData());
//			log.trace("DataPut.{},{}", key.length, value.getData().length);

//			if (handleFactory instanceof SequenceHandleFactory)
//			{
//				txn().setLastId(
//						((SequenceHandleFactory) handleFactory).getNext());
//			}
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to store hypergraph link: " + ex.toString(), ex);
		}
		return handle;
	}

	@Override
	public void addIncidenceLink(HGPersistentHandle targetHandle,
			HGPersistentHandle linkHandle)
	{
		try
		{
			byte[] key = targetHandle.toByteArray();
			byte[] value = linkHandle.toByteArray();
			incidenceDb.put(txn().getDbTransaction(), key, value, NODUPDATA);
//						System.out.println("IncidencePut." + key.length + "," + value.length);

//						if (result != OperationStatus.SUCCESS && result != OperationStatus.KEYEXIST)
//								throw new Exception("OperationStatus: " + result);

//						cursor = incidence_db.openCursor(txn().getDbTransaction(), cursorConfig);
//						OperationStatus status = cursor.getSearchBoth(key, value, LockMode.DEFAULT);
//						if (status == OperationStatus.NOTFOUND)
//						{
//								OperationStatus result = incidence_db.put(txn().getDbBTransaction(), key, value);
//								if (result != OperationStatus.SUCCESS)
//										throw new Exception("OperationStatus: " + result);
//						}
		}
		catch (Exception ex)
		{
			throw new HGException(FAILED_UPDATE_INCIDENCE_SET_FOR_HDL
					+ targetHandle + ": " + ex.toString(), ex);
		}
	}

//	@Override
//	public void addIncidenceLinks(HGPersistentHandle linkHandle, Set<HGPersistentHandle> newTargets)
//	{
//		if (newTargets.isEmpty())
//		{
//			return;
//		}
//		try
//		{
//			byte[] value = linkHandle.toByteArray();
//
//			for (HGPersistentHandle newTarget : newTargets)
//			{
//				byte[] key = newTarget.toByteArray();
//				byte[] orgValue = incidenceDb.put(txn().getDbTransaction(), key,
//						value, NODUPDATA); // NOSONAR just ignoring return value
////						System.out.println("IncidencePut." + key.length + "," + value.length);
//
////						if (result != OperationStatus.SUCCESS && result != OperationStatus.KEYEXIST)
////							throw new Exception("OperationStatus: " + result);
//			}
//		} // incidence_db.stat(txn().getDbTransaction())
//		catch (Exception ex)
//		{
//			throw new HGException(FAILED_UPDATE_INCIDENCE_SET_FOR_HDL
//					+ linkHandle + ": " + ex.toString(), ex);
//		}
//	}

	@Override
	public boolean containsLink(HGPersistentHandle handle)
	{
		byte[] key = handle.toByteArray();
		byte[] value;

		try
		{
			value = dataDb.get(txn().getDbTransaction(), key);
			if (value != null)
			{
//			System.out.println(value.toString());
				return true;
			}
		}
		catch (MDBXException ex)
		{
			throw new HGException(FAILED_RETRIEVE_LINK_WITH_HDL + handle + ": "
					+ ex.toString(), ex);
		}

		return false;
	}

	@Override
	public byte[] getData(HGPersistentHandle handle)
	{
		try
		{
			byte[] key = handle.toByteArray();
			return primitiveDb.get(txn().getDbTransaction(), key);
		}
		catch (MDBXException ex)
		{
			throw new HGException(FAILED_RETRIEVE_LINK_WITH_HDL + handle, ex);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException(
					"HGStore.getIncidenceSet called with a null handle.");

		Cursor cursor = null;
		try
		{
			byte[] key = handle.toByteArray();
			Entry entry;
			TransactionMdbxImpl tx = txn();
			cursor = incidenceDb.openCursor(tx.getDbTransaction());
			entry = cursor.get(CursorOp.SET, key);

			if (entry == null)
			{
				cursor.close();
				return (HGRandomAccessResult<HGPersistentHandle>) HGSearchResult.EMPTY;
			}
			else
			{
				return new SingleKeyResultSet<>(tx.attachCursor(cursor),
						new DatabaseEntry(entry.getKey()), 
						BAtoHandle.getInstance(store.getConfiguration().getHandleFactory()));
			}
		}
		catch (Exception ex)
		{
			closeCursor(cursor);
			throw new HGException("Failed to retrieve incidence set for handle "
					+ handle + ": " + ex.toString(), ex);
		}
	}

	@Override
	public long getIncidenceSetCardinality(HGPersistentHandle handle)
	{
		if (handle == null)
			throw new NullPointerException(
					"HGStore.getIncidenceSetCardinality called with a null handle.");

		try (Cursor cursor = incidenceDb.openCursor(txn().getDbTransaction()))
		{
			byte[] key = handle.toByteArray();
			Entry entry = cursor.get(CursorOp.SET, key);
			if (entry == null)
				return 0;
			else
				return cursor.count();
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to retrieve incidence set for handle "
					+ handle + ": " + ex.toString(), ex);
		}
	}

	@Override
	public HGPersistentHandle[] getLink(HGPersistentHandle handle)
	{
		try
		{
			byte[] key = handle.toByteArray();
			byte[] ba = dataDb.get(txn().getDbTransaction(), key);
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
			throw new HGException(FAILED_RETRIEVE_LINK_WITH_HDL + handle, ex);
		}
	}

//	@Override
//	public HGPersistentHandle getLastKeyPrimitive()
//	{
//		try (Cursor cursor = primitiveDb.openCursor(txn().getDbTransaction()))
//		{
//			Entry entry = cursor.get(CursorOp.LAST);
//			if (entry == null)
//				return null;
//			else
//			{
//				DatabaseEntry key = new DatabaseEntry(entry.getKey());
//				return linkBinding.entryToObject(key)[0];
//			}
//		}
//		catch (Exception ex)
//		{
//			throw new HGException(
//					"Failed to retrieve last key for data db: " + ex.toString(),
//					ex);
//		}
//	}
//
//	@Override
//	public HGPersistentHandle getLastKeyDataDb()
//	{
//		try (Cursor cursor = dataDb.openCursor(txn().getDbTransaction()))
//		{
//			Entry entry = cursor.get(CursorOp.LAST);
//			if (entry == null)
//				return null;
//			else
//			{
//				DatabaseEntry key = new DatabaseEntry(entry.getKey());
//				return linkBinding.entryToObject(key)[0];
//			}
//		}
//		catch (Exception ex)
//		{
//			throw new HGException(
//					"Failed to retrieve last key for data db: " + ex.toString(),
//					ex);
//		}
//	}

	@Override
	public HGTransactionFactory getTransactionFactory()
	{
		return new HGTransactionFactory()
		{
			@Override
			public HGStorageTransaction createTransaction(
					HGTransactionContext context, HGTransactionConfig config,
					HGTransaction parent)
			{
				try
				{
//					log.trace("MDBX create transact1 at:{}", System.currentTimeMillis());
					Transaction tx = null;
					if (parent != null)
					{
						checkState(!config.isReadonly(),
								"Can't create a child transaction on a read-only parent.");
						tx = env.createTransaction(
								((TransactionMdbxImpl) parent
										.getStorageTransaction())
												.getDbTransaction(),
								config.isReadonly());
					}
					else
					{
						tx = env.createTransaction(config.isReadonly());
//						log.trace("MDBX create transact2 at:{}", System.currentTimeMillis());
					}
					return new TransactionMdbxImpl(
							MdbxStorageImplementation.this, handleFactory, tx,
							env, config.isReadonly());
				}
				catch (MDBXException ex)
				{
					throw new HGException(
							"Failed to create MDBX transaction object.", ex);
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
			throw new NullPointerException(
					"HGStore.remove called with a null handle.");
		try
		{
			byte[] key = handle.toByteArray();
//			log.trace("Deleting:{}", ctString.hexDump(key.getData());
			primitiveDb.delete(txn().getDbTransaction(), key);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove value with handle " + handle
					+ ": " + ex.toString(), ex);
		}
	}

//	@Override
//	public boolean hasIncidenceLink(HGPersistentHandle keyHdl, HGPersistentHandle valHdl)
//	{
//		try (Cursor cursor = incidenceDb.openCursor(txn().getDbTransaction()))
//		{
//			byte[] key = keyHdl.toByteArray();
//			byte[] value = valHdl.toByteArray();
//			Entry entry = cursor.get(CursorOp.GET_BOTH, key, value);
//			if (entry != null)
//			{
//				return true;
//			}
//		}
//		catch (Exception ex)
//		{
//			throw new HGException(FAILED_UPDATE_INCIDENCE_SET_FOR_HDL + keyHdl
//					+ ": " + ex.toString(), ex);
//		}
//		return false;
//	}

	@Override
	public void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink)
	{
		try (Cursor cursor = incidenceDb.openCursor(txn().getDbTransaction()))
		{
			byte[] key = handle.toByteArray();
			byte[] value = oldLink.toByteArray();
			Entry entry = cursor.get(CursorOp.GET_BOTH, key, value);
			if (entry != null)
			{
				cursor.delete();
			}
		}
		catch (Exception ex)
		{
			throw new HGException(FAILED_UPDATE_INCIDENCE_SET_FOR_HDL + handle
					+ ": " + ex.toString(), ex);
		}
	}

	@Override
	public void removeIncidenceSet(HGPersistentHandle handle)
	{
		try
		{
			byte[] key = handle.toByteArray();
			incidenceDb.delete(txn().getDbTransaction(), key);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to remove incidence set of handle "
					+ handle + ": " + ex.toString(), ex);
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
			try (Database db = env.openDatabase(txn().getDbTransaction(),
												DefaultIndexImpl.DB_NAME_PREFIX + name, 0))
			{
				return db != null;
			}
			catch (MDBXException ex)
			{
				if (MDBXException.Status.NOTFOUND.getStatusCode() == ex
						.getErrorCode())
				{
					return false;
				}
				throw new HGException(ex);
			}
		}
	}


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
			
//			String name, HGIndexConfig<KeyType, ValueType> indexConfig,
//			boolean isBidirectional, boolean createIfNecessary)
	{
		indicesLock.readLock().lock();
		try
		{
			HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>) openIndices
					.get(name);
			if (idx != null)
				return idx;
			if (!createIfNecessary && !checkIndexExisting(name))
				return null;
		}
		finally
		{
			indicesLock.readLock().unlock();
		}

		indicesLock.writeLock().lock();
		try
		{
			HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>) openIndices
					.get(name);
			if (idx != null)
				return idx;

			if (!createIfNecessary && !checkIndexExisting(name))
				return null;

			DefaultIndexImpl<KeyType, ValueType> result = null;

			if (isBidirectional)
				result = new DefaultBiIndexImpl<>(name, this,
						store.getTransactionManager(), keyConverter, valueConverter);
			else
				result = new DefaultIndexImpl<>(name, this,
						store.getTransactionManager(), keyConverter, valueConverter);
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

			try (Database db = env.openDatabase(txn().getDbTransaction(),
					DefaultIndexImpl.DB_NAME_PREFIX + name, 0))
			{
				db.drop(txn().getDbTransaction(), true);
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

//	CheckPointThread checkPointThread = null;
//
//	class CheckPointThread extends Thread
//	{
//		boolean stop = false;
//		boolean running = false;
//
//		CheckPointThread()
//		{
//			setName("HGCHECKPOINT");
//			setDaemon(true);
//		}
//
//		@Override
//		public void run()
//		{
//			try
//			{
//				running = true;
//				while (!stop)
//				{
//					Thread.sleep(60_000);
//					if (!stop)
//						try
//						{
//							float percentFull = env.percentageFull();
//							if (percentFull > 80.0f)
//							{
//								log.error(getStats(Collections.emptyMap()));
//								log.error(
//										"The database is getting too full. Now at {}%",
//										percentFull);
//							}
//						}
//						catch (MDBXException ex)
//						{
//							throw new Error(ex);
//						}
//				}
//			}
//			catch (InterruptedException ex)
//			{
//				if (stop)
//					try
//					{
//						float percentFull = env.percentageFull();
//						if (percentFull > 80.0f)
//						{
//							log.error(getStats(Collections.emptyMap()));
//							log.error(
//									"The database is getting too full. Now at {}%",
//									percentFull);
//						}
//					}
//					catch (MDBXException dx)
//					{
//						throw new Error(dx);
//					}
//				else
//				{
//					log.warn(
//							"Warning: HGDB CHECKPOINT THREAD got interrupted with: {}, stack trace follows...",
//							ex.toString(), ex);
//				}
//			}
//			catch (Exception e)
//			{
//				log.error(
//						"HGDB CHECKPOINT THREAD exiting with: {}, stack trace follows...",
//						e.toString(), e);
//			}
//			finally
//			{
//				running = false;
//			}
//		}
//	}

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
	public HGDataInput newDataInput(byte[] buffer)
	{
		return MdbxDataInput.getInstance(buffer);
	}

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
	public HGDataInput newDataInput(byte[] buffer, int offset, int length)
	{
		return MdbxDataInput.getInstance(buffer, offset, length);
	}

	/**
	 * Creates a data input object from the data contained in a data output
	 * object. A reference to the data output's byte array will be kept by this
	 * object (it will not be copied) and therefore the data output object
	 * should not be modified while this object is in use.
	 *
	 * @param output
	 *            is the data output object containing the data to be read.
	 */
	public HGDataInput newDataInput(HGDataOutput output)
	{
		return MdbxDataInput.getInstance(output);
	}

	/**
	 * Creates a data output object for writing a byte array of tuple data.
	 */
	public HGDataOutput newDataOutput()
	{
		return MdbxDataOutput.getInstance();
	}

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
	public HGDataOutput newDataOutput(byte[] buffer)
	{
		return MdbxDataOutput.getInstance(buffer);
	}

	private void closeCursor(Cursor cursor)
	{
		if (cursor != null)
		{
			try
			{
				cursor.close();
			}
			catch (Exception e)
			{
				// do nothing
			}
		}
	}

	@Override
	public boolean containsData(HGPersistentHandle handle)
	{
		try (Cursor c = this.primitiveDb.openCursor(txn().getDbTransaction()))
		{
			return c.get(CursorOp.FIRST) != null;
		}
	}

	@SuppressWarnings("unchecked")
	@Override
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

//	@Override
//	public HGRandomAccessResult<HGPersistentHandle> scanAllDataKeys()
//	{
//		HGRandomAccessResult<HGPersistentHandle> result = null;
//		Cursor cursor = null;
//
//		try
//		{
//			TransactionMdbxImpl tx = txn();
//			cursor = dataDb.openCursor(tx.getDbTransaction());
//			Entry entry = cursor.get(CursorOp.FIRST);
//
//			if (entry != null)
//			{
//				HGIndexConfig<HGPersistentHandle, byte[]> indexConfig = new HGIndexConfig<>(
//						BAtoHandle.getInstance(handleFactory),
//						BAtoBA.getInstance(), null);
//				IndexKeyConverter<HGPersistentHandle, byte[]> indexKeyConverter = new IndexKeyConverter<>(
//						indexConfig, store.getTransactionManager(),
//						encryptUtils);
//				result = new KeyScanResultSet<>(tx.attachCursor(cursor),
//						new DatabaseEntry(entry.getKey()), indexKeyConverter);
//			}
//			else
//			{
//				closeCursor(cursor);
//				result = (HGRandomAccessResult<HGPersistentHandle>) HGSearchResult.EMPTY;
//			}
//		}
//		catch (Exception ex)
//		{
//			closeCursor(cursor);
//			throw new HGException(
//					"Failed to retrieve data keys" + ": " + ex.toString(), ex);
//		}
//
//		return result;
//	}

//	@Override
//	public HGRandomAccessResult<HGPersistentHandle> scanAllIncidenceKeys()
//	{
//		HGRandomAccessResult<HGPersistentHandle> result = null;
//		Cursor cursor = null;
//
//		try
//		{
//			TransactionMdbxImpl tx = txn();
//			cursor = incidenceDb.openCursor(tx.getDbTransaction());
//			Entry entry = cursor.get(CursorOp.FIRST);
//
//			if (entry != null)
//			{
//				HGIndexConfig<HGPersistentHandle, HGPersistentHandle> indexConfig = new HGIndexConfig<>(
//						BAtoHandle.getInstance(handleFactory),
//						BAtoHandle.getInstance(handleFactory), null);
//				IndexKeyConverter<HGPersistentHandle, HGPersistentHandle> indexKeyConverter = new IndexKeyConverter<>(
//						indexConfig, store.getTransactionManager(),
//						encryptUtils);
//				result = new KeyScanResultSet<>(tx.attachCursor(cursor),
//						new DatabaseEntry(entry.getKey()), indexKeyConverter);
//			}
//			else
//			{
//				closeCursor(cursor);
//				result = (HGRandomAccessResult<HGPersistentHandle>) HGSearchResult.EMPTY;
//			}
//		}
//		catch (Exception ex)
//		{
//			closeCursor(cursor);
//			throw new HGException("Failed to retrieve incidence set keys" + ": "
//					+ ex.toString(), ex);
//		}
//
//		return result;
//	}

//	@Override
//	public String getStats(Map<String, String> options)
//	{
//		StringBuilder sb = new StringBuilder();
//		Env env = getEnvironment();
//		Transaction dbTransaction = txn().getDbTransaction();
//		float percentFull;
//
//		if (dbTransaction == null)
//		{
//			sb.append("Env Version:");
//			sb.append(Env.version());
//			MDBX_stat stat = env.stat();
//			sb.append(" Stats:");
//			sb.append(stat.toString());
//			sb.append('\n');
//
//			EnvInfo info = env.info();
//			sb.append("Info:");
//			sb.append(info.toString());
//			sb.append('\n');
//
//			percentFull = env.percentageFull();
//		}
//		else
//		{
//			sb.append("Env Version:");
//			sb.append(Env.version());
//			MDBX_stat stat = env.stat(dbTransaction);
//			sb.append(" Stats:");
//			sb.append(stat.toString());
//			sb.append('\n');
//
//			EnvInfo info = env.info(dbTransaction);
//			sb.append("Info:");
//			sb.append(info.toString());
//			sb.append('\n');
//
//			stat = dataDb.stat(dbTransaction);
//			sb.append("Data Db Stats:");
//			sb.append(stat.toString());
//			sb.append('\n');
//
//			stat = primitiveDb.stat(dbTransaction);
//			sb.append("Primitive Db Stats:");
//			sb.append(stat.toString());
//			sb.append('\n');
//
//			stat = incidenceDb.stat(dbTransaction);
//			sb.append("Incidence Db Stats:");
//			sb.append(stat.toString());
//			sb.append('\n');
//
//			for (Iterator<HGIndex<?, ?>> i = openIndices.values().iterator(); i
//					.hasNext();)
//			{
//				sb.append(i.next().getStats());
//			}
//
//			boolean showAllDbs = Optional.ofNullable(options.get("ALL_DBS"))
//					.map(Boolean::parseBoolean).orElse(false);
//			if (showAllDbs)
//			{
//				Set<String> openedDbs = new HashSet<>();
//				openedDbs.add(DATA_DB_NAME);
//				openedDbs.add(INCIDENCE_DB_NAME);
//				openedDbs.add(PRIMITIVE_DB_NAME);
//
//				for (Iterator<HGIndex<?, ?>> i = openIndices.values()
//						.iterator(); i.hasNext();)
//				{
//					DefaultIndexImpl<?, ?> dfltIndex = ClassUtil
//							.uncheckedCast(i.next());
//					openedDbs.add(dfltIndex.getDatabaseName());
//					if (dfltIndex instanceof DefaultBiIndexImpl)
//					{
//						DefaultBiIndexImpl<?, ?> dfltBiIndex = ClassUtil
//								.uncheckedCast(dfltIndex);
//						openedDbs.add(dfltBiIndex.getPrimaryDatabaseName());
//					}
//				}
//
//				List<String> databases = env.listDatabases(dbTransaction);
//				databases.removeAll(openedDbs);
//
//				sb.append("Unopened Dbs:\n");
//
//				databases.stream().sorted().forEach(dbName -> {
//					Database db = env.openDatabase(dbTransaction, dbName, 0);
//					try
//					{
//						MDBX_stat dbStat = db.stat(dbTransaction);
//						sb.append('\t');
//						sb.append(dbName);
//						sb.append(':');
//						sb.append(dbStat.toString());
//						sb.append('\n');
//					}
//					finally
//					{
//						db.close();
//					}
//				});
//			}
//
//			sb.append("Cursor Pool Stats:");
//			sb.append(env.getPoolStats());
//			sb.append('\n');
//
//			percentFull = env.percentageFull(dbTransaction);
//		}
//
//		sb.append("% Full:");
//		sb.append(percentFull + "%");
//		sb.append('\n');
//
//		return sb.toString();
//	}
//
//	@SuppressWarnings("deprecation")
//	@Override
//	public float getUsedPercentage()
//	{
//		Env env = getEnvironment();
//		Transaction dbTransaction = txn().getDbTransaction();
//		float percentFull;
//
//		if (dbTransaction == null)
//		{
//			percentFull = env.percentageFull(); // NOSONAR: using deprecated if
//												// txn is missing
//		}
//		else
//		{
//			percentFull = env.percentageFull(dbTransaction);
//		}
//
//		return percentFull;
//	}

//	@Override
//	public long getMaxDbSize()
//	{
//		Env env = getEnvironment();
//		Transaction dbTransaction = txn().getDbTransaction();
//		long mapSize;
//
//		if (dbTransaction == null)
//		{
//			@SuppressWarnings("deprecation")
//			EnvInfo info = env.info(); // NOSONAR: using deprecated if txn is
//										// missing
//			mapSize = info.getMapSize();
//		}
//		else
//		{
//			EnvInfo info = env.info(dbTransaction);
//			mapSize = info.getMapSize();
//		}
//
//		return mapSize;
//	}

//	@Override
//	public List<String> getDbNames()
//	{
//		Transaction dbTransaction = txn().getDbTransaction();
//		return env.listDatabases(dbTransaction);
//	}
}