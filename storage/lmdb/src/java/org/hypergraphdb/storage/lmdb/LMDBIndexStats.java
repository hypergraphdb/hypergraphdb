package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.HGIndexStats;
import org.hypergraphdb.util.Ref;
import org.lmdbjava.Cursor;
import org.lmdbjava.GetOp;
import org.lmdbjava.Stat;

public class LMDBIndexStats<BufferType, Key, Value> implements HGIndexStats<Key, Value>
{
	DefaultIndexImpl<BufferType, Key, Value> index;
	
	public LMDBIndexStats(DefaultIndexImpl<BufferType, Key, Value> index)
	{
		this.index = index;
	}

	
	public Count entries(long cost, boolean isEstimateOk)
	{
		index.checkOpen();
		return new Count(new Ref<Long>() {
			public Long get()
			{
				try
				{
					return index.db.stat(index.txn().lmdbTxn()).entries;
				}
				catch (Exception ex)
				{
					throw new HGException(ex);
				}				
			}
		}, false);
	}
	
	public Count keys(long cost, boolean isEstimateOk)
	{
		index.checkOpen();
		if (cost < Long.MAX_VALUE && !isEstimateOk)
			return null;
		else if (cost == Long.MAX_VALUE)
			return new Count(new Ref<Long>() 
			{
				public Long get()
				{
					final long [] value = new long[1];
					try (HGRandomAccessResult<Key> keys = index.scanKeys())
					{
						while (keys.hasNext()) { value[0]++; keys.next(); }
					}
					return value[0];
				}
			}, false);					
		else // isEstimateOk
		{
			return new Count(new Ref<Long>() {
				public Long get()
				{
					Stat stat = index.db.stat(index.txn().lmdbTxn());					
					return stat.entries;
				}
			}, true);
		}
	}

	public Count values(long cost, boolean isEstimateOk)
	{
		index.checkOpen();
		if (cost < Long.MAX_VALUE && !isEstimateOk)
			return null;
		else if (cost == Long.MAX_VALUE)
			return new Count(new Ref<Long>() {
				public Long get()
				{
					final long [] cnt = new long[1];
					try (HGRandomAccessResult<Value> values = index.scanValues())
					{
						while (values.hasNext()) { cnt[0]++; values.next(); }
					}
					return cnt[0];	
				}
			}, false);
		else // isEstimateOk
		{
			return new Count(new Ref<Long>() {
				public Long get()
				{
					Stat stat = index.db.stat(index.txn().lmdbTxn());				
					return stat.entries;	
				}
			}, true);			
		}		
	}

	public Count valuesOfKey(final Key key, final long cost, final boolean isEstimateOk)
	{
		index.checkOpen();
		if (cost == 0)
			return null;
		else
		{
			Ref<Long> counter = new Ref<Long>() {
			public Long get() {
				try (Cursor<BufferType> cursor = index.db.openCursor(index.txn().lmdbTxn())) 
				{
					BufferType keybuf = index.hgBufferProxy.fromBytes(index.keyConverter.toByteArray(key));
					if (cursor.get(keybuf, GetOp.MDB_SET))
						return cursor.count();
					else
						return 0l;
				}
				catch (Exception ex)
				{
					throw new HGException(ex);
				}
			}};
			return new Count(counter, false);
		}		
	}


	public Count keysWithValue(final Value value, final long cost, final boolean isEstimateOk)
	{
		index.checkOpen();
		if (index instanceof DefaultBiIndexImpl)
			return null;
		if (cost == 0)
			return null;
		else
		{
			final DefaultBiIndexImpl<Key, Value> bindex = (DefaultBiIndexImpl<Key, Value>)index;
			Ref<Long> counter = new Ref<Long>() {
			public Long get() {				
				try (SecondaryCursor cursor = bindex.secondaryDb.openSecondaryCursor(index.txn().getDbTransaction())) 
				{
					byte [] valueAsBytes = bindex.valueConverter.toByteArray(value);
					Entry entry = cursor.get(CursorOp.SET, valueAsBytes);
					if (entry != null)
						return cursor.count();
					else
						return 0l;
				}
				catch (LMDBException ex)
				{
					throw new HGException(ex);
				}
			}};
			return new Count(counter, false);
		}		
		
	}
}
