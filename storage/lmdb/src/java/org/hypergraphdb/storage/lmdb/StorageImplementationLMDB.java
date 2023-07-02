/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c)  Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

import org.hypergraphdb.HGConfiguration;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGIndex;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.storage.BAtoBA;
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
import org.hypergraphdb.util.HGUtils;
import org.lmdbjava.BufferProxy;
import org.lmdbjava.ByteArrayProxy;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.Env;
import org.lmdbjava.EnvFlags;
import org.lmdbjava.GetOp;
import org.lmdbjava.LmdbException;
import org.lmdbjava.Txn;
import org.lmdbjava.TxnFlags;

public class StorageImplementationLMDB<BufferType> implements HGStoreImplementation
{
	private static final String DATA_DB_NAME = "datadb";
	private static final String PRIMITIVE_DB_NAME = "primitivedb";
	private static final String INCIDENCE_DB_NAME = "incidencedb";
	
	private HGStore store;
	private ConfigLMDB config = new ConfigLMDB();
	private Env<BufferType> lmdbenv = null;
	private BufferProxy<BufferType> bufferProxy;
	private HGBufferProxyLMDB<BufferType> hgBufferProxy;
	private Dbi<BufferType> primitive_db = null;
	private Dbi<BufferType> data_db = null;
	private Dbi<BufferType> incidence_db = null;
    private HashMap<String, HGIndex<?, ?>> openIndices = new HashMap<String, HGIndex<?, ?>>();
    private ReentrantReadWriteLock indicesLock = new ReentrantReadWriteLock();
	
	@SuppressWarnings("unchecked")
	private StorageTransactionLMDB<BufferType> txn()
	{
		HGTransaction tx = store.getTransactionManager().getContext().getCurrent();
		return (StorageTransactionLMDB<BufferType>)(tx == null ? 
				StorageTransactionLMDB.nullTransaction() :
				tx.getStorageTransaction());
	}
	
    private void ensureOpen()
    {
        if (lmdbenv == null)
            throw new IllegalStateException("StorageImplementationLMDB is either closed or was never initialized.");
    }
	    
    boolean checkIndexExisting(String name)
    {
        if (openIndices.get(name) != null)
            return true;
        else
        {
            Dbi<BufferType> db = null;
            try
            {
                String db_filename = DefaultIndexImpl.DB_NAME_PREFIX + name;
                return (new File(this.store.getDatabaseLocation(),  db_filename)).exists();
//                db = lmdbenv.openDbi .openDbi(db_filename);
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
    
    <T> T inReadTxn(Function<Txn<BufferType>, T> f)
    {
        StorageTransactionLMDB<BufferType> current = txn();
        if (current.lmdbTxn() != null)
            return f.apply(current.lmdbTxn());
        else
        {
            try (Txn<BufferType> tx = lmdbEnv().txnRead())
            {
                return f.apply(tx);
            }
        }
    }
    
    <T> T inWriteTxn(Function<Txn<BufferType>, T> f)
    {
        StorageTransactionLMDB<BufferType> current = txn();
        if (current.lmdbTxn() != null && !current.lmdbTxn().isReadOnly())
            return f.apply(current.lmdbTxn());
        else
        {
            try (Txn<BufferType> tx = lmdbEnv().txnWrite())
            {
                T x = f.apply(tx);
                tx.commit();
                return x;
            }
        }
    }
        
    public StorageImplementationLMDB()
    {
        this.bufferProxy = (BufferProxy<BufferType>) ByteArrayProxy.PROXY_BA; 
        this.hgBufferProxy = null;
    }
    
	public StorageImplementationLMDB(BufferProxy<BufferType> byteBufferProxy,
									HGBufferProxyLMDB<BufferType> hgBufferProxy)
	{
		this.bufferProxy = byteBufferProxy;
		this.hgBufferProxy = hgBufferProxy;		
	}

	@Override
	public ConfigLMDB getConfiguration()
	{
		return this.config;
	}
	
	public Env<BufferType> lmdbEnv()
	{
		return this.lmdbenv;
	}
	
	@Override
	public void startup(HGStore store, HGConfiguration hgconfig)
	{	
		this.store = store;
		if (this.hgBufferProxy == null)
		    this.hgBufferProxy = (HGBufferProxyLMDB<BufferType>)new HGByteArrayBufferProxyLMDB(hgconfig.getHandleFactory());
		lmdbenv = Env.create(this.bufferProxy)
			.setMaxDbs(10000)
//			.setMapSize(10485760*100)
			.setMapSize(0)
			.open(new File(store.getDatabaseLocation()), 
				  EnvFlags.MDB_NOSYNC);
		
		primitive_db = lmdbenv.openDbi(PRIMITIVE_DB_NAME, DbiFlags.MDB_CREATE);
		data_db = lmdbenv.openDbi(DATA_DB_NAME, DbiFlags.MDB_CREATE);
		incidence_db = lmdbenv.openDbi(INCIDENCE_DB_NAME, DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT);
	}

	@Override
	public HGTransactionFactory getTransactionFactory()
	{	    
		return new HGTransactionFactory()
		{
			@SuppressWarnings("unchecked")
			@Override
			public HGStorageTransaction createTransaction(HGTransactionContext context, 
														  HGTransactionConfig config,
														  HGTransaction parent)
			{
				try
				{
					Txn<BufferType> tx = null;
					if (config.isNoStorage())
						return new VanillaTransaction();
					if (config.isReadonly())
						tx = lmdbenv.txn(parent == null ? null : ((StorageTransactionLMDB<BufferType>)parent.getStorageTransaction()).lmdbTxn(), TxnFlags.MDB_RDONLY_TXN);
					else
						tx = lmdbenv.txn(parent == null ? null : ((StorageTransactionLMDB<BufferType>)parent.getStorageTransaction()).lmdbTxn());
							
					return new StorageTransactionLMDB<BufferType>(tx, StorageImplementationLMDB.this.lmdbenv);
				}
				catch (LmdbException ex)
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
	public HGPersistentHandle store(HGPersistentHandle handle, byte[] data)
	{
	    ensureOpen();	    
	    return this.inWriteTxn(tx -> {
    		BufferType keybuf = this.hgBufferProxy.fromHandle(handle);
    		BufferType databuf = this.hgBufferProxy.fromBytes(data);
    		primitive_db.put(txn().lmdbTxn(), keybuf, databuf);
    		return handle;
	    });
	}
	
	@Override
	public byte[] getData(HGPersistentHandle handle)
	{
	    ensureOpen();
		return hgBufferProxy.toBytes(primitive_db.get(txn().lmdbTxn(), this.hgBufferProxy.fromHandle(handle)));
	}
	
	@Override
	public boolean containsData(HGPersistentHandle handle)
	{
	    ensureOpen();
		try (Cursor<BufferType> cursor = primitive_db.openCursor(txn().lmdbTxn()))
		{
			return cursor.get(this.hgBufferProxy.fromHandle(handle), GetOp.MDB_SET);
		}
	}

	@Override
	public void removeData(HGPersistentHandle handle)
	{
	    ensureOpen();	    
		primitive_db.delete(txn().lmdbTxn(), this.hgBufferProxy.fromHandle(handle));
	}

	@Override
	public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
	{
	    ensureOpen();	    
	    try
	    {
	        return this.inWriteTxn(tx -> {
        		BufferType keybuf = this.hgBufferProxy.fromHandle(handle);
        		BufferType databuf = this.hgBufferProxy.fromHandleArray(link);
        		data_db.put(tx, keybuf, databuf);
        		return handle;
	        });
	    }
	    catch (Exception ex)
	    {
	        throw new HGException(ex);
	    }
	}
	
	@Override
	public HGPersistentHandle[] getLink(HGPersistentHandle handle)
	{
	    ensureOpen();	    
		return hgBufferProxy.toHandleArray(data_db.get(txn().lmdbTxn(), 
		        this.hgBufferProxy.fromHandle(handle)));
	}


	@Override
	public void removeLink(HGPersistentHandle handle)
	{
	    ensureOpen();	    
		data_db.delete(txn().lmdbTxn(), this.hgBufferProxy.fromHandle(handle));		
	}
	
	@Override
	public boolean containsLink(HGPersistentHandle handle)
	{
	    ensureOpen();	    
		try (Cursor<BufferType> cursor = data_db.openCursor(txn().lmdbTxn()))
		{			
			return cursor.get(this.hgBufferProxy.fromHandle(handle), GetOp.MDB_SET);
		}
	}
	
	@Override
	public void addIncidenceLink(HGPersistentHandle atom, HGPersistentHandle link)
	{
	    ensureOpen();
		BufferType keybuf = this.hgBufferProxy.fromHandle(atom);
		BufferType databuf = this.hgBufferProxy.fromHandle(link);
		incidence_db.put(txn().lmdbTxn(), keybuf, databuf);
	}

	@SuppressWarnings("unchecked")
	@Override
	public HGRandomAccessResult<HGPersistentHandle> getIncidenceResultSet(HGPersistentHandle atom)
	{
	    ensureOpen();
		Cursor<BufferType> cursor = null;
		try 
		{
			cursor = incidence_db.openCursor(txn().lmdbTxn());
			BufferType key = hgBufferProxy.fromHandle(atom); 
			if (!cursor.get(key, GetOp.MDB_SET))
				return (HGRandomAccessResult<HGPersistentHandle>) HGSearchResult.EMPTY;
			else
				return new SingleKeyResultSet<BufferType, HGPersistentHandle>(
						new LMDBTxCursor<BufferType>(cursor, txn()),
						this.hgBufferProxy.fromHandle(atom),
						BAtoHandle.getInstance(store.getConfiguration().getHandleFactory()),
						this.hgBufferProxy);
		}
		catch (Exception ex)
		{
			if (cursor != null)
				try { cursor.close(); } catch (Throwable t) { }
			if (ex instanceof HGException)
				throw (HGException)ex;
			else
				throw new HGException(ex);
		}
		
	}

	@Override
	public long getIncidenceSetCardinality(HGPersistentHandle atom)
	{
	    ensureOpen();
		try (Cursor<BufferType> c = incidence_db.openCursor(txn().lmdbTxn()))
		{
			if (!c.get(hgBufferProxy.fromHandle(atom), GetOp.MDB_SET_KEY))
				return 0;
			else
				return c.count();
		}
	}

	@Override
	public void removeIncidenceLink(HGPersistentHandle atom, HGPersistentHandle link)
	{
	    ensureOpen();
		incidence_db.delete(txn().lmdbTxn(), 
							this.hgBufferProxy.fromHandle(atom), 
							this.hgBufferProxy.fromHandle(link));
	}

	@Override
	public void removeIncidenceSet(HGPersistentHandle atom)
	{
	    ensureOpen();
		incidence_db.delete(txn().lmdbTxn(), this.hgBufferProxy.fromHandle(atom));
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(String name)
	{	   
        ensureOpen();
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

	@Override
	@SuppressWarnings("unchecked")
	public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
			String name, ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter, Comparator<byte[]> keyComparator,
			Comparator<byte[]> valueComparator, boolean isBidirectional, boolean createIfNecessary)
	{
        ensureOpen();

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

            DefaultIndexImpl<BufferType, KeyType, ValueType> result = null;

            if (isBidirectional)
            {
                result = new DefaultBiIndexImpl<BufferType, KeyType, ValueType>(name, this, store.getTransactionManager(), keyConverter,
                        valueConverter, keyComparator, valueComparator, this.hgBufferProxy);
            }
            else
            {
                result = new DefaultIndexImpl<BufferType, KeyType, ValueType>(name, this, store.getTransactionManager(), keyConverter,
                        valueConverter, keyComparator, valueComparator, this.hgBufferProxy);
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
	
	@Override
	public void removeIndex(String name)
	{
	    ensureOpen();
        indicesLock.writeLock().lock();
        try
        {
            HGIndex<?, ?> idx = openIndices.get(name);
            if (idx != null)
            {
                Dbi<BufferType> db = ((DefaultIndexImpl)idx).db;
                Dbi<BufferType> secondaryDb = (idx instanceof DefaultBiIndexImpl) ? 
                        ((DefaultBiIndexImpl)idx).secondaryDb : null;                
                // droping will close, we don't need to call idx.close here
                try (Txn<BufferType> tx = lmdbenv.txnWrite())
                {
                    db.drop(tx, true);
                    if (secondaryDb != null) 
                    {
                        db.drop(tx, true);
                    }               
                    tx.commit();
                }
                catch (Exception e)
                {
                    throw new HGException(e);
                }                
                openIndices.remove(name);
            }

        }
        finally
        {
            indicesLock.writeLock().unlock();
        }
	}

	@Override
	public void shutdown()
	{
		try { this.lmdbenv.close(); }
		catch (Throwable t) { t.printStackTrace(System.err); }
		finally { this.lmdbenv = null; }
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
	
	static void checkBasicStoreOperations(StorageImplementationLMDB<byte[]> storageImpl, HGConfiguration config, HGStore store)
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
	
	static void checkMultipleValuePerKeyInIndex(HGIndex index, HGStore store)
	{
        store.getTransactionManager().ensureTransaction(() -> {
            index.addEntry("hello".getBytes(), "bonjour".getBytes());
            index.addEntry("hello".getBytes(), "salut".getBytes());
            index.addEntry("hello".getBytes(), "ciao".getBytes());
            try (HGSearchResult<byte[]> rs = index.find("hello".getBytes()))
            {
                while (rs.hasNext())
                {
                    System.out.println("Hello -> " + new String(rs.next()));
                }
            }
            return null;
        });
	    
	}
	
	public static void main(String [] argv)
	{
		File location = new File("/home/borislav/temp/hgdbtemp/data");
		HGUtils.dropHyperGraphInstance(location.getAbsolutePath());
		location.mkdirs();
		HGConfiguration config = new HGConfiguration();
		StorageImplementationLMDB<byte[]> storageImpl = 
				new StorageImplementationLMDB<byte[]>(ByteArrayProxy.PROXY_BA, 
						new HGByteArrayBufferProxyLMDB(config.getHandleFactory()));
		config.setStoreImplementation(storageImpl);
		HGStore store = new HGStore(location.getAbsolutePath(), config);
		try
		{
			storageImpl.startup(store, config);
//			checkBasicStoreOperations(storageImpl, config, store);
            HGIndex index = store.getTransactionManager().ensureTransaction(() -> {
                return store.getIndex("theindex", BAtoBA.getInstance(), BAtoBA.getInstance(), null, null, true);			
            });
//			checkMultipleValuePerKeyInIndex(index, store);
//			store.getTransactionManager().ensureTransaction(() -> {
			    store.removeIndex("theindex");
//			    return null;
//			});
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
