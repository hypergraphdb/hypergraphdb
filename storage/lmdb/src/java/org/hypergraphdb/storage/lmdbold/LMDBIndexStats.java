package org.hypergraphdb.storage.lmdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.HGIndexStats;
import org.hypergraphdb.util.Ref;
import org.fusesource.lmdbjni.Cursor;
import org.fusesource.lmdbjni.CursorOp;
import org.fusesource.lmdbjni.Database;
import org.fusesource.lmdbjni.Entry;
import org.fusesource.lmdbjni.JNI.MDB_stat;
import org.fusesource.lmdbjni.LMDBException;
import org.fusesource.lmdbjni.SecondaryCursor;

public class LMDBIndexStats<Key, Value> implements HGIndexStats<Key, Value>
{
	DefaultIndexImpl<Key, Value> index;
	
	public LMDBIndexStats(DefaultIndexImpl<Key, Value> index)
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
					long cnt = index.db.stat(index.txn().getDbTransaction()).ms_entries;
					if (index.isSplitIndex())
					{
						cnt += index.db2.stat(index.txn().getDbTransaction()).ms_entries;
					}
					return cnt;
				}
				catch (LMDBException ex)
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
					MDB_stat stat = index.db.stat();					
					return stat.ms_entries;
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
					MDB_stat stat = index.db.stat();					
					return stat.ms_entries;	
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
				Database db = index.keyBucket(key) == 0 ? index.db : index.db2;
				try (Cursor cursor = db.openCursor(index.txn().getDbTransaction())) 
				{
					byte[] keyAsBytes = index.keyConverter.toByteArray(key);
					Entry entry = cursor.get(CursorOp.SET, keyAsBytes);
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
