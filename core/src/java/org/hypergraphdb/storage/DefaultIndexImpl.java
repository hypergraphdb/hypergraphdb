/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage;

import java.util.Comparator;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.transaction.TransactionBDBImpl;
import org.hypergraphdb.transaction.VanillaTransaction;

import com.sleepycat.db.BtreeStats;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.DatabaseType;
import com.sleepycat.db.Cursor;
import com.sleepycat.db.LockMode;
import com.sleepycat.db.DatabaseConfig;
import com.sleepycat.db.DatabaseEntry;
import com.sleepycat.db.Environment;
import com.sleepycat.db.OperationStatus;

/**
 * <p>
 * A default index implementation. This implementation works by maintaining
 * a separate DB, using a B-tree, <code>byte []</code> lexicographical ordering
 * on its keys. The keys are therefore assumed to by <code>byte [] </code>
 * instances.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class DefaultIndexImpl<KeyType, ValueType> implements HGSortIndex<KeyType, ValueType>
{
	/**
	 * Prefix of HyperGraph index DB filenames.
	 */
    public static final String DB_NAME_PREFIX = "hgstore_idx_";
        
    protected Environment env;
    protected HGTransactionManager transactionManager;
    protected String name;
    protected Database db;
    private   boolean owndb;
    protected Comparator comparator;
    protected boolean sort_duplicates = true;
    protected ByteArrayConverter<KeyType> keyConverter;
    protected ByteArrayConverter<ValueType> valueConverter;
    
    protected void checkOpen()
    {
        if (!isOpen())
            throw new HGException("Attempting to operate on index '" + 
                                  name + 
                                  "' while the index is being closed.");              
    }

    protected TransactionBDBImpl txn()
    {
    	HGTransaction tx = transactionManager.getContext().getCurrent();
    	if (tx == null || tx.getStorageTransaction() instanceof VanillaTransaction)
    		return TransactionBDBImpl.nullTransaction();
    	else
    		return (TransactionBDBImpl)tx.getStorageTransaction();
    }

    public DefaultIndexImpl(Environment env,
    						Database db,
							HGTransactionManager transactionManager,
							ByteArrayConverter<KeyType> keyConverter,
							ByteArrayConverter<ValueType> valueConverter,
							Comparator comparator)
	{
		this.db = db;
		this.env = env;
		this.transactionManager = transactionManager;
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
		this.comparator = comparator;
		owndb = false;
		try { name = db.getDatabaseName(); }
		catch (Exception ex) { throw new HGException(ex); }
		
	}
    
    public DefaultIndexImpl(String indexName, 
    						Environment env,
    						HGTransactionManager transactionManager,
    						ByteArrayConverter<KeyType> keyConverter,
    						ByteArrayConverter<ValueType> valueConverter,
    						Comparator comparator)
    {
        this.name = indexName;
        this.env = env;
        this.transactionManager = transactionManager;
        this.keyConverter = keyConverter;
        this.valueConverter = valueConverter;
        this.comparator = comparator;
        owndb = true;
    }
    
    public String getName()
    {
        return name;
    }
    
    public String getDatabaseName()
    {
        return DB_NAME_PREFIX + name;
    }
    
    public Comparator getComparator()
    {
        return comparator;
    }
    
    public void open()
    {    	
        try
        {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setAllowCreate(true);
            if (env.getConfig().getTransactional())
            	dbConfig.setTransactional(true);
            dbConfig.setType(DatabaseType.BTREE);
            dbConfig.setSortedDuplicates(sort_duplicates);
            if (comparator != null)
           		dbConfig.setBtreeComparator(comparator);                	
            db = env.openDatabase(null, DB_NAME_PREFIX + name, null, dbConfig);
        }
        catch (Throwable t)
        {
            throw new HGException("While attempting to open index ;" + 
                                  name + "': " + t.toString(), t);
        }
    }

    public void close()
    {
    	if (db == null || !owndb)
    		return;
        try
        {
            db.close();
        }
        catch(Throwable t)
        {
            throw new HGException(t);
        }
        finally
        {
            db = null;            
        }
    }
    
    public boolean isOpen()
    {
        return db != null;
    }
    
    public HGRandomAccessResult<ValueType> scanValues()
    {
        checkOpen();
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();        
        HGRandomAccessResult result = null;
        Cursor cursor = null;
        try
        {
        	TransactionBDBImpl tx = txn();
            cursor = db.openCursor(tx.getBDBTransaction(), null);
            OperationStatus status = cursor.getFirst(keyEntry, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS && cursor.count() > 0)
                result = new KeyRangeForwardResultSet(tx.attachCursor(cursor), keyEntry, valueConverter);
            else 
            {
                try { cursor.close(); } catch (Throwable t) { }
                result = HGSearchResult.EMPTY;
            }                
        }
        catch (Throwable ex)
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }
        	throw new HGException("Failed to lookup index '" + 
                                  name + "': " + ex.toString(), 
                                  ex);
        }
        return result;        
    }

    public HGRandomAccessResult<KeyType> scanKeys()
    {
        checkOpen();
        DatabaseEntry keyEntry = new DatabaseEntry();
        DatabaseEntry value = new DatabaseEntry();        
        HGRandomAccessResult result = null;
        Cursor cursor = null;       
        try
        {
        	TransactionBDBImpl tx = txn();
            cursor = db.openCursor(tx.getBDBTransaction(), null);
            OperationStatus status = cursor.getFirst(keyEntry, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS && cursor.count() > 0)
                result = new KeyScanResultSet(tx.attachCursor(cursor), keyEntry, keyConverter);
            else 
            {
                try { cursor.close(); } catch (Throwable t) { }
                result = HGSearchResult.EMPTY;
            }                
        }
        catch (Throwable ex)
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }
        	throw new HGException("Failed to lookup index '" + 
                                  name + "': " + ex.toString(), 
                                  ex);
        }
        return result;        
    }
    
    public void addEntry(KeyType key, ValueType value)
    {
        checkOpen();
        DatabaseEntry dbkey = new DatabaseEntry(keyConverter.toByteArray(key));
        DatabaseEntry dbvalue = new DatabaseEntry(valueConverter.toByteArray(value)); 
        try
        {
            OperationStatus result = db.putNoDupData(txn().getBDBTransaction(), dbkey, dbvalue);
            if (result != OperationStatus.SUCCESS && result != OperationStatus.KEYEXIST)
                throw new Exception("OperationStatus: " + result);            
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to add entry to index '" + 
                                  name + "': " + ex.toString(), ex);
        }
    }

    public void removeEntry(KeyType key, ValueType value)
    {
        checkOpen();
/*        if (key == null)
            throw new HGException("Attempting to lookup index '" + 
                                  name + "' with a null key."); */
        DatabaseEntry keyEntry = new DatabaseEntry(keyConverter.toByteArray(key));
        DatabaseEntry valueEntry = new DatabaseEntry(valueConverter.toByteArray(value));
        Cursor cursor = null;
        try
        {
            cursor = db.openCursor(txn().getBDBTransaction(), null);
            if (cursor.getSearchBoth(keyEntry, valueEntry, LockMode.DEFAULT) == OperationStatus.SUCCESS)
                cursor.delete();
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to lookup index '" + 
                                  name + "': " + ex.toString(), 
                                  ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }
        }
    }

    public void removeAllEntries(KeyType key)
    {
        checkOpen();
        DatabaseEntry dbkey = new DatabaseEntry(keyConverter.toByteArray(key));
        try
        {
            db.delete(txn().getBDBTransaction(), dbkey);
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to delete entry from index '" + 
                                  name + "': " + ex.toString(), ex);
        }
    }
    
    public ValueType findFirst(KeyType key)
    {
        checkOpen();
/*        if (key == null)
            throw new HGException("Attempting to lookup index '" + 
                                  name + "' with a null key."); */
        DatabaseEntry keyEntry = new DatabaseEntry(keyConverter.toByteArray(key));
        DatabaseEntry value = new DatabaseEntry();        
        ValueType result = null;
        Cursor cursor = null;
        try
        {
            cursor = db.openCursor(txn().getBDBTransaction(), null);
            OperationStatus status = cursor.getSearchKey(keyEntry, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
               result = valueConverter.fromByteArray(value.getData());
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to lookup index '" + 
                                  name + "': " + ex.toString(), 
                                  ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }
        }
        return result;
    }

    /**
     * <p>
     * Find the last entry, assuming ordered duplicates, corresponding to the 
     * given key.
     * </p>
     * 
     * @param key The key whose last entry is sought.
     * @return The last (i.e. greatest, i.e. maximum) data value for that key
     * or null if the set of entries for the key is empty.
     */
    public ValueType findLast(KeyType key)
    {
        checkOpen();
        DatabaseEntry keyEntry = new DatabaseEntry(keyConverter.toByteArray(key));
        DatabaseEntry value = new DatabaseEntry();        
        ValueType result = null;
        Cursor cursor = null;
        try
        {
            cursor = db.openCursor(txn().getBDBTransaction(), null);            
            OperationStatus status = cursor.getLast(keyEntry, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
               result = valueConverter.fromByteArray(value.getData());
        }
        catch (Exception ex)
        {
            throw new HGException("Failed to lookup index '" + 
                                  name + "': " + ex.toString(), 
                                  ex);
        }
        finally
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }
        }
        return result;
    }
    
    public HGRandomAccessResult<ValueType> find(KeyType key)
    {
        checkOpen();
/*        if (key == null)
            throw new HGException("Attempting to lookup index '" + 
                                  name + "' with a null key."); */
        DatabaseEntry keyEntry = new DatabaseEntry(keyConverter.toByteArray(key));
        DatabaseEntry value = new DatabaseEntry();        
        HGRandomAccessResult result = null;
        Cursor cursor = null;
        try
        {
        	TransactionBDBImpl tx = txn();
            cursor = db.openCursor(txn().getBDBTransaction(), null);
            OperationStatus status = cursor.getSearchKey(keyEntry, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS && cursor.count() > 0)
                result = new SingleKeyResultSet(tx.attachCursor(cursor), keyEntry, valueConverter);
            else 
            {
                try { cursor.close(); } catch (Throwable t) { }
                result = HGSearchResult.EMPTY;
            }
        }
        catch (Throwable ex)
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }            
            throw new HGException("Failed to lookup index '" + 
                                  name + "': " + ex.toString(), 
                                  ex);
        }
        return result;        
    }
    
    private HGRandomAccessResult<ValueType> findOrdered(KeyType key, boolean lower_range, boolean compare_equals)
    {
        checkOpen();
/*        if (key == null)
            throw new HGException("Attempting to lookup index '" + 
                                  name + "' with a null key."); */
        byte [] keyAsBytes = keyConverter.toByteArray(key);
        DatabaseEntry keyEntry = new DatabaseEntry(keyAsBytes);
        DatabaseEntry value = new DatabaseEntry();
        Cursor cursor = null;
        try
        {
        	TransactionBDBImpl tx = txn();
            cursor = db.openCursor(txn().getBDBTransaction(), null);
            OperationStatus status = cursor.getSearchKeyRange(keyEntry, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
            {       
            	Comparator<byte[]> comparator = db.getConfig().getBtreeComparator();
                if (!compare_equals)
                {
                    if (lower_range)
                        status = cursor.getPrev(keyEntry, value, LockMode.DEFAULT);
                    else if (comparator.compare(keyAsBytes, keyEntry.getData()) == 0)
                    	status = cursor.getNextNoDup(keyEntry, value, LockMode.DEFAULT);
                }
                else if (lower_range && comparator.compare(keyAsBytes, keyEntry.getData()) != 0)
                	status = cursor.getPrev(keyEntry, value, LockMode.DEFAULT);
            }

            if (status == OperationStatus.SUCCESS)
	            if (lower_range)
	                return new KeyRangeBackwardResultSet(tx.attachCursor(cursor), keyEntry, valueConverter);
	            else
	                return new KeyRangeForwardResultSet(tx.attachCursor(cursor), keyEntry, valueConverter);
            else
                try { cursor.close(); } catch (Throwable t) { }
                return (HGRandomAccessResult<ValueType>)HGSearchResult.EMPTY;
        }
        catch (Throwable ex)
        {
            if (cursor != null)
                try { cursor.close(); } catch (Throwable t) { }            
            throw new HGException("Failed to lookup index '" + 
                                  name + "': " + ex.toString(), 
                                  ex);
        }
    }
    
    public HGRandomAccessResult<ValueType> findGT(KeyType key)
    {
        return findOrdered(key, false, false);
    }

    public HGRandomAccessResult<ValueType> findGTE(KeyType key)
    {
        return findOrdered(key, false, true);
    }

    public HGRandomAccessResult<ValueType> findLT(KeyType key)
    {
        return findOrdered(key, true, false);
    }

    public HGRandomAccessResult<ValueType> findLTE(KeyType key)
    {
        return findOrdered(key, true, true);
    }

    protected void finalize()
    {
        if (isOpen())
            try { close(); } catch (Throwable t) { }
    }

	public long count()
	{
		try
		{
			return ((BtreeStats)db.getStats(txn().getBDBTransaction(), null)).getNumKeys();
		}
		catch (DatabaseException ex)
		{
			throw new HGException(ex);
		}
	}

	public long count(KeyType key)
	{
        Cursor cursor = null;
        try
        {
            cursor = db.openCursor(txn().getBDBTransaction(), null);
            DatabaseEntry keyEntry = new DatabaseEntry(keyConverter.toByteArray(key));
            DatabaseEntry value = new DatabaseEntry();                    
            OperationStatus status = cursor.getSearchKey(keyEntry, value, LockMode.DEFAULT);
            if (status == OperationStatus.SUCCESS)
            	return cursor.count();
            else
            	return 0;
        }
        catch (DatabaseException ex)
        {
        	throw new HGException(ex);
        }
        finally
        {
        	if (cursor != null)        
        		try { cursor.close(); } catch (Throwable t) { }
        }
	}    
}
