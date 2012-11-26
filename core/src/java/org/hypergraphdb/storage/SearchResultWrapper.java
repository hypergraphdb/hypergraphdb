package org.hypergraphdb.storage;

import org.hypergraphdb.HGSearchResult;

/**
 * <p>
 * Used to wrap a {@see HGRandomAccessResult} in order to "disable" it. This is
 * need in certain cases where the underlying storage result set is not ordered
 * or random access, for example when an index is queried with range queries (lt, gt etc.).
 * </p>
 * 
 * @author borislav
 *
 * @param <T>
 */
public final class SearchResultWrapper<T> implements HGSearchResult<T>
{
	private HGSearchResult<T> rs = null;
	
	public SearchResultWrapper(HGSearchResult<T> rs)
	{
		this.rs = rs;
	}

	public boolean hasNext()
	{
		return rs.hasNext();
	}

	public T next()
	{
		return rs.next();
	}

	public void remove()
	{
		rs.remove();
	}

	public void close()
	{
		rs.close();
	}

	public T current()
	{
		return rs.current();
	}

	public boolean isOrdered()
	{
		return rs.isOrdered();
	}

	public boolean hasPrev()
	{
		return rs.hasPrev();
	}

	public T prev()
	{
		return rs.prev();
	}	
}