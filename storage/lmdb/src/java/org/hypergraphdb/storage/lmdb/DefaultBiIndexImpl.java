/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import static org.hypergraphdb.storage.lmdb.LMDBUtils.checkArgNotNull;

import java.nio.charset.StandardCharsets;
import java.util.Comparator;

import org.hypergraphdb.HGBidirectionalIndex;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.util.HGUtils;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.GetOp;
import org.lmdbjava.PutFlags;
import org.lmdbjava.Txn;

@SuppressWarnings("unchecked")
public class DefaultBiIndexImpl<BufferType, KeyType, ValueType>
		extends DefaultIndexImpl<BufferType, KeyType, ValueType>
		implements HGBidirectionalIndex<KeyType, ValueType>
{
	private static final String SECONDARY_DB_NAME_PREFIX = DB_NAME_PREFIX
			+ "_secondary";
	Dbi<BufferType> secondaryDb = null;

	public DefaultBiIndexImpl(String indexName,
			StorageImplementationLMDB<BufferType> storage,
			HGTransactionManager transactionManager,
			ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter,
			Comparator<byte[]> keyComparator,
			Comparator<byte[]> valueComparator,
			HGBufferProxyLMDB<BufferType> hgBufferProxy)
	{
		super(indexName, 
			  storage, 
			  transactionManager, 
			  keyConverter,
			  valueConverter,
			  keyComparator,
			  valueComparator,
			  hgBufferProxy);
	}

    public Comparator<BufferType> getValueComparator()
    {
        return new Comparator<BufferType>() 
        {
            public int compare(BufferType left, BufferType right)           
            {
                if (valueComparator != null)
                    return valueComparator.compare(hgBufferProxy.toBytes(left), hgBufferProxy.toBytes(right));
                else
                    return fastcompare(hgBufferProxy.toBytes(left), hgBufferProxy.toBytes(right));
            }
        };
    }
    
	public void open()
	{
		super.open();
		try
		{			
            Txn<BufferType> tx = txn().lmdbTxn();
            if (tx != null)		    
    			this.secondaryDb = storage.lmdbEnv().openDbi(txn().lmdbTxn(),
    					(SECONDARY_DB_NAME_PREFIX + getName()).getBytes(StandardCharsets.UTF_8), 
    					this.getValueComparator(), 
    					false,
    					DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT);
            else
                this.secondaryDb = storage.lmdbEnv().openDbi(
                        (SECONDARY_DB_NAME_PREFIX + getName()).getBytes(StandardCharsets.UTF_8), 
                        this.getValueComparator(), 
                        false,
                        DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT);                
		}
		catch (Throwable t)
		{
			throw new HGException("While attempting to open index ;" + name
					+ "': " + t.toString(), t);
		}
	}

	public void close()
	{
		HGException exception = null;

		try
		{
			super.close();
		}
		catch (HGException ex)
		{
			exception = ex;
		}

		if (secondaryDb == null)
			return;

		// Attempt to close secondary database even if there was an exception
		// during the close of the primary.
		try
		{
			secondaryDb.close();
		}
		catch (Throwable t)
		{
			if (exception == null)
				exception = new HGException(t);
		}
		finally
		{
			secondaryDb = null;
		}

		if (exception != null)
			throw exception;
	}

	public boolean isOpen()
	{
		return super.isOpen() && secondaryDb != null;
	}

	public void addEntry(KeyType key, ValueType value)
	{
		checkOpen();
		try
		{
		    storage.inWriteTxn(tx -> {
		        db.put(tx, 
					this.hgBufferProxy.fromBytes(keyConverter.toByteArray(key)), 
					this.hgBufferProxy.fromBytes(valueConverter.toByteArray(value)), 
					PutFlags.MDB_NODUPDATA);
		        secondaryDb.put(tx, 					
					this.hgBufferProxy.fromBytes(valueConverter.toByteArray(value)),
					this.hgBufferProxy.fromBytes(keyConverter.toByteArray(key)),					
					PutFlags.MDB_NODUPDATA);
		        return null;
		    });
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to add entry to index '" + name
					+ "': " + ex.toString(), ex);
		}
	}

	public HGRandomAccessResult<KeyType> findByValue(ValueType value)
	{
		checkOpen();
		checkArgNotNull(value, "value");
		LMDBTxCursor<BufferType> cursor = null;
		
		BufferType valuebuf = this.hgBufferProxy.fromBytes(valueConverter.toByteArray(value));
		HGRandomAccessResult<KeyType> result = null;
		
		try
		{
			cursor = new LMDBTxCursor<BufferType>(secondaryDb.openCursor(txn().lmdbTxn()), txn());
			if (cursor.cursor().get(valuebuf, GetOp.MDB_SET))
				result = new SingleKeyResultSet<BufferType, KeyType>(
						cursor,
						valuebuf,
						keyConverter,
						this.hgBufferProxy);
			else
			{
				HGUtils.closeNoException(cursor);
				result = (HGRandomAccessResult<KeyType>) HGSearchResult.EMPTY;
			}
		}
		catch (Throwable ex)
		{
			HGUtils.closeNoException(cursor);
			System.out.println("Inner Exception:");
			ex.printStackTrace();
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	public KeyType findFirstByValue(ValueType value)
	{
		checkOpen();
	    return storage.inReadTxn(tx -> {
    		try (Cursor<BufferType> cursor = secondaryDb.openCursor(tx))
    		{
    			if (cursor.get(this.hgBufferProxy.fromBytes(valueConverter.toByteArray(value)), 
    							GetOp.MDB_SET))
    			{
    				byte [] data = this.hgBufferProxy.toBytes(cursor.val());
    				return keyConverter.fromByteArray(data, 0, data.length);
    			}
    			return null;
    		}
    		catch (Exception ex)
    		{
    			throw new HGException("In database findFirstByValue for " + this.db.getName(), ex);
    		}
	    });
	}

	public long countKeys(ValueType value)
	{	
		checkOpen();
	    return storage.inReadTxn(tx -> {
    		try (Cursor<BufferType> cursor = secondaryDb.openCursor(tx))
    		{
    			if (cursor.get(this.hgBufferProxy.fromBytes(valueConverter.toByteArray(value)), 
    							GetOp.MDB_SET))
    			{
    				return cursor.count();
    			}
    			else
    				return 0L;
    		}
    		catch (Exception ex)
    		{
    			throw new HGException("In database findFirstByValue for " + this.db.getName(), ex);
    		}
	    });
	}
}
