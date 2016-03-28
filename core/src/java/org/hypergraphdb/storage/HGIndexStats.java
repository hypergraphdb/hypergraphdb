package org.hypergraphdb.storage;

import org.hypergraphdb.util.Ref;

/**
 * <p>
 * Provides count or count estimates for the number of key/value pairs currently in an
 * index (an associative array). The premise is that an implementation may be able
 * to provide a given count at no computational cost or at some computational cost 
 * that is less than the cost of scanning the full index. Moreover, it may be the 
 * case that an implementation cannot provide an exact count at a reasonable 
 * cost, but can be provide a good enough estimate. Frequently, such counts are examined
 * for query planning and estimates are all that is necessary.
 * </p>
 * 
 * <p>
 * In general, an implementation may be able to provide either an estimate or an exact
 * count at a reasonable computational cost. Because only the caller knows what an acceptable
 * cost would be, it is up to them to indicate it to the storage implementation. So all
 * methods take a cost and an <code>isEstimateOk</code> flag as arguments. An implementation
 * will return a non-null {@link org.hypergraphdb.storage.HGIndexStats.Count} object only
 * if it can provide a value at or below the indicate cost. When the <code>isEstimateOk</code>
 * parameter is set to true, an implementation will also try to return an estimate if an exact
 * value cannot be provided at the stated cost. 
 * </p>
 * 
 * <p>
 * The <code>cost</code> parameter in the method in this interface has a relatively loose
 * definition: 0 means "no cost", the value must be immediately available, 1 means that the storage
 * sub-system can be accessed once, 2 means it can be accessed twice...Long.MAX_VALUE means that
 * any cost is acceptable, including a full scan.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public interface HGIndexStats<Key, Value>
{
	/**
	 * <p>
	 * A count value - simply wraps a long with a flag whether it's just an estimate
	 * or an exact value.
	 * </p>
	 * 
	 * @author borislav
	 *
	 */
	public class Count
	{
		private boolean isEstimate = false;
		private Ref<Long> counter = null;
		public Count(Ref<Long> counter, boolean isEstimate) { this.counter = counter; this.isEstimate = isEstimate; }
		public boolean isEstimate() { return isEstimate; }
		public long value() { return counter.get().longValue(); }
	}

	/**
	 * <p>Return the total number of entries (key/value pairs) in the index, including
	 * duplicates. See this interface description
	 * for the meaning of the <code>cost</code> and <code>isEstimateOk<code> arguments.</p>
	 */
	//default Count entries(long cost, boolean isEstimateOk) { return null; }
	Count entries(long cost, boolean isEstimateOk);
	
	/**
	 * <p>Return the number of keys in the index. See this interface description
	 * for the meaning of the <code>cost</code> and <code>isEstimateOk<code> arguments.</p>
	 */
	//default Count keys(long cost, boolean isEstimateOk) { return null; }
	Count keys(long cost, boolean isEstimateOk);
	
	/**
	 * <p>Return the number of values stored for a given key in the index. See this interface description
	 * for the meaning of the <code>cost</code> and <code>isEstimateOk<code>  arguments.</p>
	 */
	//default Count valuesOfKey(Key key, long cost, boolean isEstimateOk) { return null; }
	Count valuesOfKey(Key key, long cost, boolean isEstimateOk);
	
	/**
	 * <p>For {@link org.hypergraphdb.HGBidirectionalIndex}es, return the number of values in 
	 * the index. See this interface description for the meaning of the 
	 * <code>cost</code> and <code>isEstimateOk<code> arguments.</p>
	 */
	//default Count values(long cost, boolean isEstimateOk) { return null; }
	Count values(long cost, boolean isEstimateOk);
	
	/**
	 * <p>For {@link org.hypergraphdb.HGBidirectionalIndex}es, return the number of keys for which 
	 * the specified <code>value</code> is a value. See this interface description for 
	 * the meaning of the <code>cost</code> and <code>isEstimateOk<code> arguments.</p>
	 */
	//default Count keysWithValue(Value value, long cost, boolean isEstimateOk) { return null; }
	Count keysWithValue(Value value, long cost, boolean isEstimateOk);	
}