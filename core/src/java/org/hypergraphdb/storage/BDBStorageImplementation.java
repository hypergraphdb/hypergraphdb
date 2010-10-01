package org.hypergraphdb.storage;

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
import org.hypergraphdb.transaction.HGStorageTransaction;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionContext;
import org.hypergraphdb.transaction.HGTransactionFactory;
import org.hypergraphdb.transaction.TransactionBDBImpl;
import org.hypergraphdb.transaction.VanillaTransaction;

import com.sleepycat.db.Cursor;
import com.sleepycat.db.CursorConfig;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.Environment;
import com.sleepycat.db.EnvironmentConfig;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.OperationStatus;
import com.sleepycat.db.TransactionConfig;

public class BDBStorageImplementation implements HGStoreImplementation
{
//    static
//    {
//        String osname = System.getProperty("os.name");
//        if (osname.indexOf("win") > -1 || osname.indexOf("Win") > -1)
//        {
//              System.loadLibrary("msvcm80");
//              System.loadLibrary("msvcr80");
//              System.loadLibrary("msvcp80");          
//            System.loadLibrary("libdb50");
//            System.loadLibrary("libdb_java50"); 
//        }
//    }
    
    private static final String DATA_DB_NAME = "datadb";
    private static final String PRIMITIVE_DB_NAME = "primitivedb";
    private static final String INCIDENCE_DB_NAME = "incidencedb";
     
    private BDBConfig configuration;
    private HGStore store;
    private HGHandleFactory handleFactory;
    private CursorConfig cursorConfig = new CursorConfig();
    private Environment env = null;
    private Database data_db = null;
    private Database primitive_db = null;
    private Database incidence_db = null;
    private HashMap<String, HGIndex<?,?>> openIndices = new HashMap<String, HGIndex<?,?>>();
    private ReentrantReadWriteLock indicesLock = new ReentrantReadWriteLock();    
    private LinkBinding linkBinding = null;
    
    private TransactionBDBImpl txn()
    {
        HGTransaction tx = store.getTransactionManager().getContext().getCurrent();;
        if (tx == null || tx.getStorageTransaction() instanceof VanillaTransaction)
            return TransactionBDBImpl.nullTransaction();
        else
            return (TransactionBDBImpl)tx.getStorageTransaction();
    }    
    
    public BDBStorageImplementation()
    {
        configuration = new BDBConfig();
    }
    
    public BDBConfig getConfiguration()
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
//      configuration.setStorageMVCC(false);
        if (config.isTransactional())
            configuration.configureTransactional();
        File envDir = new File(store.getDatabaseLocation());
        envDir.mkdirs();
        try
        {
            env = new Environment(envDir, envConfig);
            data_db = env.openDatabase(null, DATA_DB_NAME, null, configuration.getDatabaseConfig().cloneConfig());    
            primitive_db = env.openDatabase(null, PRIMITIVE_DB_NAME, null, configuration.getDatabaseConfig().cloneConfig());
            
            DatabaseConfig incConfig = configuration.getDatabaseConfig().cloneConfig();
            incConfig.setSortedDuplicates(true);
            incidence_db = env.openDatabase(null, INCIDENCE_DB_NAME, null, incConfig);
            
            if (config.isTransactional())
            {
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
                try { Thread.sleep(500); }
                catch (InterruptedException ex) { /* need to wait here until it stops... */}
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
            for (Iterator<HGIndex<?,?>> i = openIndices.values().iterator(); i.hasNext(); )
                try
                {
                    i.next().close();
                }
                catch (Throwable t)
                {
                    // TODO - we need to log the exception here, once we've decided
                    // on a logging mechanism. 
                    t.printStackTrace();
                }
            try { data_db.close(); }
            catch (Throwable t) { t.printStackTrace(); }
            
            try { primitive_db.close(); }
            catch (Throwable t) { t.printStackTrace(); }
            
            try { incidence_db.close(); }
            catch (Throwable t) { t.printStackTrace(); }
            
            try { env.close(); }
            catch (Throwable t) { t.printStackTrace(); }
        }         
    }
    
    public void removeLink(HGPersistentHandle handle)
    {
        if (handle == null)
            throw new NullPointerException("HGStore.remove called with a null handle.");
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            data_db.delete(txn().getBDBTransaction(), key);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to remove value with handle " + handle + 
                    ": " + ex.toString(), ex);            
        }
    }

    public void store(HGPersistentHandle handle, byte[] data)
    {
        try
        {
            OperationStatus result = primitive_db.put(txn().getBDBTransaction(), 
                                                 new DatabaseEntry(handle.toByteArray()), 
                                                 new DatabaseEntry(data));
            if (result != OperationStatus.SUCCESS)
                throw new Exception("OperationStatus: " + result);            
            
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to store hypergraph raw byte []: " + ex.toString(), ex);
        }
    }

    public HGPersistentHandle store(HGPersistentHandle handle, HGPersistentHandle[] link)
    {
        DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
        DatabaseEntry value = new DatabaseEntry(); 
        linkBinding.objectToEntry(link, value);
        try
        {
            OperationStatus result = data_db.put(txn().getBDBTransaction(), key, value);
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
            OperationStatus result = incidence_db.putNoDupData(txn().getBDBTransaction(), key, value);
            if (result != OperationStatus.SUCCESS && result != OperationStatus.KEYEXIST)
                throw new Exception("OperationStatus: " + result);            
            
//            cursor = incidence_db.openCursor(txn().getBDBTransaction(), cursorConfig);
//            OperationStatus status = cursor.getSearchBoth(key, value, LockMode.DEFAULT);
//            if (status == OperationStatus.NOTFOUND)
//            {
//                OperationStatus result = incidence_db.put(txn().getBDBTransaction(), key, value);
//                if (result != OperationStatus.SUCCESS)
//                    throw new Exception("OperationStatus: " + result);
//            }
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to update incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Exception ex) { }
        }  
    }

    public boolean containsLink(HGPersistentHandle handle)
    {        
        DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
        DatabaseEntry value = new DatabaseEntry();
        try
        {
            if (data_db.get(txn().getBDBTransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)          
            {
//                System.out.println(value.toString());
                return true;
            }
        } catch (DatabaseException ex)
        {
            throw new HGException("Failed to retrieve link with handle " + handle + 
                    ": " + ex.toString(), ex);
        }      
        
        return false;
    }

    public byte[] getData(HGPersistentHandle handle)
    {
        try
        {            
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();
            if (primitive_db.get(txn().getBDBTransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)          
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
            TransactionBDBImpl tx = txn();
            cursor = incidence_db.openCursor(tx.getBDBTransaction(), cursorConfig);            
            OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND)
            {
                cursor.close();
                return (HGRandomAccessResult<HGPersistentHandle>)HGSearchResult.EMPTY;
            }
            else
                return new SingleKeyResultSet(tx.attachCursor(cursor), 
                                              key, 
                                              BAtoHandle.getInstance(handleFactory));            
        }
        catch (Throwable ex)
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }                        
            throw new HGException("Failed to retrieve incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
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
            cursor = incidence_db.openCursor(txn().getBDBTransaction(), cursorConfig);
            OperationStatus status = cursor.getSearchKey(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.NOTFOUND)
                return 0;
            else
                return cursor.count();
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to retrieve incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }       
        finally
        {
            try { cursor.close(); } catch (Throwable t) { }
        }
    }

    public HGPersistentHandle[] getLink(HGPersistentHandle handle)
    {
        try
        {           
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry();
            if (data_db.get(txn().getBDBTransaction(), key, value, LockMode.DEFAULT) == OperationStatus.SUCCESS)          
                return (HGPersistentHandle [])linkBinding.entryToObject(value);
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
            public HGStorageTransaction createTransaction(HGTransactionContext context, HGTransaction parent)
            {           
                try
                {
                    TransactionConfig tconfig = new TransactionConfig();
                    if (env.getConfig().getMultiversion())                    
                        tconfig.setSnapshot(true);
                    tconfig.setWriteNoSync(true);
//                  tconfig.setNoSync(true);
                    if (parent != null)
                        return new TransactionBDBImpl(env.beginTransaction(((TransactionBDBImpl)parent.getStorageTransaction()).getBDBTransaction(), tconfig), 
                                                      env);
                    else
                        return new TransactionBDBImpl(env.beginTransaction(null, tconfig), 
                                                      env); 
                }
                catch (DatabaseException ex)
                {
//                  System.err.println("Failed to create transaction, will exit - temporary behavior to be removed at some point.");
                    ex.printStackTrace(System.err);
//                  System.exit(-1);
                    throw new HGException("Failed to create BerkeleyDB transaction object.", ex);
                }
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
            primitive_db.delete(txn().getBDBTransaction(), key);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to remove value with handle " + handle + 
                    ": " + ex.toString(), ex);            
        }
    }

    public void removeIncidenceLink(HGPersistentHandle handle, HGPersistentHandle oldLink)
    {
        Cursor cursor = null;
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            DatabaseEntry value = new DatabaseEntry(oldLink.toByteArray());
            cursor = incidence_db.openCursor(txn().getBDBTransaction(), cursorConfig);
            OperationStatus status = cursor.getSearchBoth(key, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
            {
                cursor.delete();
            }
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to update incidence set for handle " + handle + 
                                  ": " + ex.toString(), ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Exception ex) { }
        }  
    }

    public void removeIncidenceSet(HGPersistentHandle handle)
    {
        try
        {
            DatabaseEntry key = new DatabaseEntry(handle.toByteArray());
            incidence_db.delete(txn().getBDBTransaction(), key);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to remove incidence set of handle " + handle + 
                    ": " + ex.toString(), ex);            
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
            DatabaseConfig cfg = new DatabaseConfig();
            cfg.setAllowCreate(false);
            Database db = null;
            try
            {
                db = env.openDatabase(null, DefaultIndexImpl.DB_NAME_PREFIX + name, null, cfg);
            }
            catch (Exception ex)
            {
            }
            if (db != null)             
            {
                try { db.close(); } catch (Throwable t) { t.printStackTrace(); }
                return true;
            }
            else
                return false;
        }       
    }    
    
    @SuppressWarnings("unchecked")
    public <KeyType, ValueType> HGIndex<KeyType, ValueType> getIndex(
            String name, ByteArrayConverter<KeyType> keyConverter,
            ByteArrayConverter<ValueType> valueConverter,
            Comparator<?> comparator, 
            boolean isBidirectional,
            boolean createIfNecessary)
    {
        indicesLock.readLock().lock();
        try
        {
            HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>)openIndices.get(name);
            if (idx != null)
                return idx;
            if (!checkIndexExisting(name) && !createIfNecessary)
                return null;
        }
        finally {indicesLock.readLock().unlock(); }
        
        indicesLock.writeLock().lock();
        try
        {
            HGIndex<KeyType, ValueType> idx = (HGIndex<KeyType, ValueType>)openIndices.get(name);
            if (idx != null)
                return idx;
            if (!checkIndexExisting(name) && !createIfNecessary)
                return null;
            
            DefaultIndexImpl<KeyType, ValueType> result = null;
            
            if (isBidirectional)
                result =  new DefaultBiIndexImpl<KeyType, ValueType>(name, 
                                                                     this, 
                                                                     store.getTransactionManager(),
                                                                     keyConverter, 
                                                                     valueConverter,
                                                                     comparator);
            else
                result = new DefaultIndexImpl<KeyType, ValueType>(name, 
                                                                  this, 
                                                                  store.getTransactionManager(),
                                                                  keyConverter, 
                                                                  valueConverter,
                                                                  comparator);
            result.open();          
            openIndices.put(name, result);
            return result;
        }
        finally
        {
            indicesLock.writeLock().unlock();
        }
    }

    
    @SuppressWarnings("unchecked")
    public void removeIndex(String name)
    {
        indicesLock.writeLock().lock();
        try
        {
            HGIndex idx = openIndices.get(name);
            if (idx != null)
            {
                idx.close();
                openIndices.remove(name);
            }
            try
            {
                env.removeDatabase(null, DefaultIndexImpl.DB_NAME_PREFIX + name, null);
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
        
        public void run()
        {
            try
            {
                running = true;
                while (!stop)
                {
                    Thread.sleep(60000);
                    if (!stop)
                        try { env.checkpoint(null); }
                        catch (DatabaseException ex) { throw new Error(ex); }
                }
            }
            catch (InterruptedException ex)
            {
                if (stop)
                    try { env.checkpoint(null); }
                    catch (DatabaseException dx) { throw new Error(dx); }                   
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
