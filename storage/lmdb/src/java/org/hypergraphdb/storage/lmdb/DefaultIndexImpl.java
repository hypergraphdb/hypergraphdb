/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.storage.lmdb;

import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.text.MessageFormat;
import java.util.Comparator;
import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGSortIndex;
import org.hypergraphdb.storage.ByteArrayConverter;
import org.hypergraphdb.storage.SearchResultWrapper;
import org.hypergraphdb.transaction.HGTransaction;
import org.hypergraphdb.transaction.HGTransactionManager;
import org.hypergraphdb.util.HGUtils;
import org.lmdbjava.Cursor;
import org.lmdbjava.Dbi;
import org.lmdbjava.DbiFlags;
import org.lmdbjava.GetOp;
import org.lmdbjava.PutFlags;
import org.lmdbjava.SeekOp;
import org.lmdbjava.Txn;

import static org.hypergraphdb.storage.lmdb.LMDBUtils.checkArgNotNull;


/**
 * <p>
 * A default index implementation. This implementation works by maintaining a
 * separate DB, using a B-tree, <code>byte []</code> lexicographical ordering on
 * its keys. The keys are therefore assumed to by <code>byte [] </code>
 * instances.
 * </p>
 * 
 * @author Borislav Iordanov
 */
@SuppressWarnings("unchecked")
public class DefaultIndexImpl<BufferType, KeyType, ValueType>
		implements HGSortIndex<KeyType, ValueType>
{
	/**
	 * Prefix of HyperGraph index DB filenames.
	 */
	public static final String DB_NAME_PREFIX = "hgstore_idx_";

	protected StorageImplementationLMDB<BufferType> storage;
	protected HGTransactionManager transactionManager;
	protected String name;
	Dbi<BufferType> db;
	protected Comparator<byte[]> keyComparator;
	protected Comparator<byte[]> valueComparator; // ignored
	protected ByteArrayConverter<KeyType> keyConverter;
	protected ByteArrayConverter<ValueType> valueConverter;
	protected HGBufferProxyLMDB<BufferType> hgBufferProxy;
	
	protected void checkOpen()
	{
		if (!isOpen())
			throw new HGException("Attempting to operate on index '" + name
					+ "' while the index is being closed.");
	}

	protected StorageTransactionLMDB<BufferType> txn()
	{
		HGTransaction tx = transactionManager.getContext().getCurrent();
		return (StorageTransactionLMDB<BufferType>)(tx == null ? 
				StorageTransactionLMDB.nullTransaction() :
				tx.getStorageTransaction());
	}
	
	public DefaultIndexImpl(String indexName, StorageImplementationLMDB<BufferType> storage,
			HGTransactionManager transactionManager,
			ByteArrayConverter<KeyType> keyConverter,
			ByteArrayConverter<ValueType> valueConverter,
			Comparator<byte[]> keyComparator,
			Comparator<byte[]> valueComparator,
			HGBufferProxyLMDB<BufferType> hgBufferProxy)
	{
		this.name = indexName;
		this.storage = storage;
		this.transactionManager = transactionManager;
		this.keyConverter = keyConverter;
		this.valueConverter = valueConverter;
		this.hgBufferProxy = hgBufferProxy;
		this.keyComparator = keyComparator;
		this.valueComparator = valueComparator;
	}

	public String getName()
	{
		return name;
	}

	public String getDatabaseName()
	{
		return DB_NAME_PREFIX + name;
	}

	public Comparator<BufferType> getKeyComparator()
	{
		return new Comparator<BufferType>() 
		{
			public int compare(BufferType left, BufferType right)			
			{
				if (keyComparator != null)
					return keyComparator.compare(hgBufferProxy.toBytes(left), hgBufferProxy.toBytes(right));
				else
					return fastcompare(hgBufferProxy.toBytes(left), hgBufferProxy.toBytes(right));
			}
		};
	}

	public void open()
	{
		try
		{
		    Txn<BufferType> tx = txn().lmdbTxn();
		    if (tx != null)
    			db = storage.lmdbEnv().openDbi(
    			        tx,
    					(DB_NAME_PREFIX + name).getBytes(StandardCharsets.UTF_8), 
    					this.getKeyComparator(),
    					false,
    					DbiFlags.MDB_CREATE, DbiFlags.MDB_DUPSORT);
		    else
		        db = storage.lmdbEnv().openDbi(
                        (DB_NAME_PREFIX + name).getBytes(StandardCharsets.UTF_8), 
                        this.getKeyComparator(),
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
		if (db == null)
			return;

		try
		{
			db.close();
		}
		catch (Throwable t)
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
		HGRandomAccessResult<ValueType> result = null;
		LMDBTxCursor<BufferType> cursor = null;

		try
		{
			cursor = new LMDBTxCursor<BufferType>(db.openCursor(txn().lmdbTxn()), txn());
			if (cursor.cursor().seek(SeekOp.MDB_FIRST))
                result = new KeyRangeForwardResultSet<BufferType, ValueType>(
                        cursor,
                        cursor.cursor().key(),
                        valueConverter,
                        this.hgBufferProxy);
			else
			{
			    HGUtils.closeNoException(cursor);
				result = (HGRandomAccessResult<ValueType>) HGSearchResult.EMPTY;
			}
		}
		catch (Throwable ex)
		{
			if (cursor != null)
			    HGUtils.closeNoException(cursor);
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	public HGRandomAccessResult<KeyType> scanKeys()
	{
		checkOpen();
		HGRandomAccessResult<KeyType> result = null;
		LMDBTxCursor<BufferType> cursor = null;
		try
		{
			cursor = new LMDBTxCursor<BufferType>(db.openCursor(txn().lmdbTxn()), txn());
			if (cursor.cursor().first())
			{
				return new KeyScanResultSet<BufferType, KeyType>(
								cursor,
								cursor.cursor().key(), 
								keyConverter,
								this.hgBufferProxy);
			}
			else
			{
				HGUtils.closeNoException(cursor);
				result = (HGRandomAccessResult<KeyType>) HGSearchResult.EMPTY;
			}
		}
		catch (Throwable ex)
		{
			HGUtils.closeNoException(cursor);
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
		return result;
	}

	public void addEntry(KeyType key, ValueType value)
	{
		checkOpen();
		try
		{
		    storage.inWriteTxn(tx -> 
		        db.put(tx, 
					this.hgBufferProxy.fromBytes(keyConverter.toByteArray(key)), 
					this.hgBufferProxy.fromBytes(valueConverter.toByteArray(value)), 
					PutFlags.MDB_NODUPDATA)
		      );
		}
		catch (Exception ex)
		{
			String msg = MessageFormat.format(
					"Failed to add entry (key:{0},data:{1}) to index {2}: {3}",
					key, value, name, ex.toString());
			throw new HGException(msg, ex);
		}
	}

	public void removeEntry(KeyType key, ValueType value)
	{
		checkOpen();
		if (key == null || value == null)
			return;

		try
		{
		    storage.inWriteTxn(tx ->
			    db.delete(tx, 
					  this.hgBufferProxy.fromBytes(keyConverter.toByteArray(key)),
					  this.hgBufferProxy.fromBytes(valueConverter.toByteArray(value)))
			);
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
	}

	public void removeAllEntries(KeyType key)
	{
		checkOpen();
		if (key == null)
			return;

		byte[] dbkey = keyConverter.toByteArray(key);
		try
		{
		    storage.inWriteTxn(tx ->
			    db.delete(tx, this.hgBufferProxy.fromBytes(dbkey))
			);
		}
		catch (Exception ex)
		{
			throw new HGException("Failed to delete entry from index '" + name
					+ "': " + ex.toString(), ex);
		}
	}

//	void ping(Transaction tx)
//	{
//		byte[] key = new byte[1];
//		try
//		{
//			db.get(tx, key);
//		}
//		catch (Exception ex)
//		{
//			throw new HGException(
//					"Failed to ping index '" + name + "': " + ex.toString(),
//					ex);
//		}
//	}

	public ValueType getData(KeyType keyType)
	{
		checkOpen();
		byte[] key = keyConverter.toByteArray(keyType);

		try
		{
		    return storage.inReadTxn(tx -> {
    			BufferType value = db.get(txn().lmdbTxn(), this.hgBufferProxy.fromBytes(key));
    			if (value != null)
    			{
    				byte [] data = this.hgBufferProxy.toBytes(value);
    				return valueConverter.fromByteArray(data, 0, data.length);
    			}
    			return null;
		    });
		}
		catch (Exception ex)
		{
			throw new HGException(
					"Failed to lookup index '" + name + "': " + ex.toString(),
					ex);
		}
	}

	public ValueType findFirst(KeyType keyType)
	{
		checkOpen();
		return storage.inReadTxn(tx -> {
    		try (Cursor<BufferType> cursor = db.openCursor(tx))
    		{
    			if (cursor.get(this.hgBufferProxy.fromBytes(keyConverter.toByteArray(keyType)), 
    							GetOp.MDB_SET))
    			{
    				byte [] data = this.hgBufferProxy.toBytes(cursor.val());
    				return valueConverter.fromByteArray(data, 0, data.length);
    			}
    			return null;
    		}
    		catch (Exception ex)
    		{
    			throw new HGException("In database findFirst for " + this.db.getName(), ex);
    		}
		});
	}

	/**
	 * <p>
	 * Find the last entry, assuming ordered duplicates, corresponding to the
	 * given key.
	 * </p>
	 * 
	 * @param keyType
	 *            The key whose last entry is sought.
	 * @return The last (i.e. greatest, i.e. maximum) data value for that key or
	 *         null if the set of entries for the key is empty.
	 */
	public ValueType findLast(KeyType keyType)
	{		
		checkOpen();
        return storage.inReadTxn(tx -> {		
    		try (Cursor<BufferType> cursor = db.openCursor(txn().lmdbTxn()))
    		{			
    			if (cursor.get(this.hgBufferProxy.fromBytes(keyConverter.toByteArray(keyType)), 
    							GetOp.MDB_SET) && cursor.last())
    			{
    				byte [] data = this.hgBufferProxy.toBytes(cursor.val());
    				return valueConverter.fromByteArray(data, 0, data.length);
    			}
    			return null;
    		}
    		catch (Exception ex)
    		{
    			throw new HGException("In database findFirst for " + this.db.getName(), ex);
    		}
        });
	}

	public HGRandomAccessResult<ValueType> find(KeyType keyType)
	{
		checkOpen();
		checkArgNotNull(keyType, "keyType");
		LMDBTxCursor<BufferType> cursor = null;
		
		BufferType key = this.hgBufferProxy.fromBytes(keyConverter.toByteArray(keyType));
		HGRandomAccessResult<ValueType> result = null;
		
		try
		{
			cursor = new LMDBTxCursor<BufferType>(db.openCursor(txn().lmdbTxn()), txn());
			if (cursor.cursor().get(key, GetOp.MDB_SET))
				return new SingleKeyResultSet<BufferType, ValueType>(
						cursor,
						key,
						valueConverter,
						this.hgBufferProxy);
			else
			{
				HGUtils.closeNoException(cursor);
				result = (HGRandomAccessResult<ValueType>) HGSearchResult.EMPTY;
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

	@SuppressWarnings({ "resource" })
	private HGSearchResult<ValueType> findOrdered(KeyType keyType,
												  boolean lower_range, 
												  boolean compare_equals)
	{
		checkOpen();
		/*
		 * if (key == null) throw new HGException("Attempting to lookup index '"
		 * + name + "' with a null key.");
		 */
		BufferType key = this.hgBufferProxy.fromBytes(keyConverter.toByteArray(keyType));
		LMDBTxCursor<BufferType> cursor = null;

		try
		{
			cursor = new LMDBTxCursor<BufferType>(db.openCursor(txn().lmdbTxn()), txn());
			boolean has_data = cursor.cursor().get(key, GetOp.MDB_SET_RANGE); 
			if (has_data)
			{
				Comparator<BufferType> comparator = getKeyComparator();
				boolean found_exact_key = comparator.compare(key, cursor.cursor().key()) == 0;
				if (!compare_equals) // strict < or >?
				{
					if (lower_range)
						has_data = cursor.cursor().seek(SeekOp.MDB_PREV);
					else if (found_exact_key)
						has_data = cursor.cursor().seek(SeekOp.MDB_NEXT_NODUP);
				}
				// Lmdb cursor will position on the key or on the next element
				// greater than the key
				// in the latter case we need to back up by one for < (or <=)
				// query
				else if (lower_range)
				{
				    if (!found_exact_key)
				        has_data = cursor.cursor().seek(SeekOp.MDB_PREV);
				    else
				    {
				        if (cursor.cursor().seek(SeekOp.MDB_NEXT_NODUP))
				            cursor.cursor().seek(SeekOp.MDB_PREV);
				        else
				            cursor.cursor().seek(SeekOp.MDB_LAST);
				    }
				}
			}
			else if (lower_range)
				has_data = cursor.cursor().seek(SeekOp.MDB_LAST);
			else
				has_data = cursor.cursor().seek(SeekOp.MDB_FIRST);

			if (has_data)
				if (lower_range)
					return new SearchResultWrapper<ValueType>(
							new KeyRangeBackwardResultSet<BufferType, ValueType>(
									cursor,
									key,
									valueConverter,
									this.hgBufferProxy));
				else
					return new SearchResultWrapper<ValueType>(
							new KeyRangeForwardResultSet<BufferType, ValueType>(
									cursor,
									key,
									valueConverter,
									this.hgBufferProxy));
			else
			{
				HGUtils.closeNoException(cursor);
				return (HGSearchResult<ValueType>) HGSearchResult.EMPTY;
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
	}

	public HGSearchResult<ValueType> findGT(KeyType key)
	{
		return findOrdered(key, false, false);
	}

	public HGSearchResult<ValueType> findGTE(KeyType key)
	{
		return findOrdered(key, false, true);
	}

	public HGSearchResult<ValueType> findLT(KeyType key)
	{
		return findOrdered(key, true, false);
	}

	public HGSearchResult<ValueType> findLTE(KeyType key)
	{
		return findOrdered(key, true, true);
	}

	public long count()
	{
		return stats().entries(Long.MAX_VALUE, false).value();
	}

	public long count(KeyType keyType)
	{
		return stats().valuesOfKey(keyType, Long.MAX_VALUE, false).value();
	}

	@Override
	public LMDBIndexStats<BufferType, KeyType, ValueType> stats()
	{
		return new LMDBIndexStats<BufferType, KeyType, ValueType>(this);
	}
	
	// what follows is code copied from the Google Guava package: specifically the 
	// UnsignedBytes.lexicographicalComparator is copied here with all its dependency bits
	// and pieces as the fastcompare method.
	// see https://github.com/google/guava/blob/master/guava/src/com/google/common/primitives/UnsignedBytes.java
	public static final int LONG_BYTES = Long.SIZE / Byte.SIZE;
	
    static final sun.misc.Unsafe theUnsafe;

    /** The offset to the first element in a byte array. */
    static final int BYTE_ARRAY_BASE_OFFSET;

    static {
      theUnsafe = getUnsafe();

      BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);

      // sanity check - this should never fail
      if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
        throw new AssertionError();
      }
}
    static final boolean BIG_ENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.BIG_ENDIAN);
    private static final int UNSIGNED_MASK = 0xFF;
    
//    private static long flip(long a) 
//    {
//        return a ^ Long.MIN_VALUE;
//    }
    
    public static int compare(long a, long b) 
    {
    	a ^= Long.MIN_VALUE; // flip
    	b ^= Long.MIN_VALUE; // flip        
        return (a < b) ? -1 : ((a > b) ? 1 : 0);
    }
    
    public static int toInt(byte value) {
        return value & UNSIGNED_MASK;
    }
    
    public static int compare(byte a, byte b) {
        return toInt(a) - toInt(b);
    }
    
	public int fastcompare(byte[] left, byte[] right) {
        int minLength = Math.min(left.length, right.length);
        int minWords = minLength / LONG_BYTES;

        /*
         * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower
         * than comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially
         * faster on 64-bit.
         */
        for (int i = 0; i < minWords * LONG_BYTES; i += LONG_BYTES) {
          long lw = theUnsafe.getLong(left, BYTE_ARRAY_BASE_OFFSET + (long) i);
          long rw = theUnsafe.getLong(right, BYTE_ARRAY_BASE_OFFSET + (long) i);
          if (lw != rw) {
            if (BIG_ENDIAN) {
              return compare(lw, rw);
            }

            /*
             * We want to compare only the first index where left[index] != right[index]. This
             * corresponds to the least significant nonzero byte in lw ^ rw, since lw and rw are
             * little-endian. Long.numberOfTrailingZeros(diff) tells us the least significant
             * nonzero bit, and zeroing out the first three bits of L.nTZ gives us the shift to get
             * that least significant nonzero byte.
             */
            int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
            return ((int) ((lw >>> n) & UNSIGNED_MASK)) - ((int) ((rw >>> n) & UNSIGNED_MASK));
          }
        }

        // The epilogue to cover the last (minLength % 8) elements.
        for (int i = minWords * LONG_BYTES; i < minLength; i++) {
          int result = compare(left[i], right[i]);
          if (result != 0) {
            return result;
          }
        }
        return left.length - right.length;
	}
	
	private static sun.misc.Unsafe getUnsafe() {
        try {
          return sun.misc.Unsafe.getUnsafe();
        } catch (SecurityException e) {
          // that's okay; try reflection instead
        }
        try {
          return java.security.AccessController.doPrivileged(
              new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
                @Override
                public sun.misc.Unsafe run() throws Exception {
                  Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                  for (java.lang.reflect.Field f : k.getDeclaredFields()) {
                    f.setAccessible(true);
                    Object x = f.get(null);
                    if (k.isInstance(x)) {
                      return k.cast(x);
                    }
                  }
                  throw new NoSuchFieldError("the Unsafe");
                }
              });
        } catch (java.security.PrivilegedActionException e) {
          throw new RuntimeException("Could not initialize intrinsics", e.getCause());
        }
}
}
