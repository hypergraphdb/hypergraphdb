package org.hypergraphdb.storage.bdb;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGRandomAccessResult;
import org.hypergraphdb.storage.HGIndexStats;
import org.hypergraphdb.storage.HGIndexStats.Count;
import org.hypergraphdb.util.Ref;

import com.sleepycat.db.BtreeStats;
import com.sleepycat.db.Database;
import com.sleepycat.db.DatabaseException;
import com.sleepycat.db.StatsConfig;

public class BDBIndexStats<Key, Value> implements HGIndexStats<Key, Value>
{
	private Database db;
	
	private  BtreeStats stats(boolean fast)
	{
		StatsConfig statsConfig = new StatsConfig();
		statsConfig.setFast(fast);
		try
		{
			return (BtreeStats)db.getStats(null, statsConfig);
		}
		catch (DatabaseException e)
		{
			throw new HGException(e);
		}		
	}
	
	public BDBIndexStats(Database db)
	{
		this.db = db;
	}
	
	public Count entries(long cost, boolean isEstimateOk)
	{ 
		if (cost < Long.MAX_VALUE && !isEstimateOk)
			return null;
		else
			return new Count(() -> {
				return (long)stats(isEstimateOk).getNumData();
			}, isEstimateOk);
	}

	public Count keys(long cost, boolean isEstimateOk)
	{
		if (isEstimateOk)
		{
			return new Count(() -> 
			{
				long count = (long)stats(true).getNumKeys();
				if (count == 0 && cost > 2)
					count = (long)stats(false).getNumKeys(); 
				return count;
			}, true);
		}
		else if (cost > 0)
		{
			return new Count(() -> 
			{
				return (long)stats(false).getNumKeys();
			}, false);
		}
		else
			return null;
	}

	public Count valuesOfKey(Key key, long cost, boolean isEstimateOk)
	{
		return null;
	}

	public Count values(long cost, boolean isEstimateOk)
	{
		return null;
	}

	public Count keysWithValue(Value value, long cost, boolean isEstimateOk)
	{
		return null;
	}
}
