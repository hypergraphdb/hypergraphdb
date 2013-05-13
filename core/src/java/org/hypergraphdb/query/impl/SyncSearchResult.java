package org.hypergraphdb.query.impl;

import org.hypergraphdb.HGSearchResult;

public class SyncSearchResult<T> implements HGSearchResult<T>
{
    private AsyncSearchResult<T> async;

    public SyncSearchResult(AsyncSearchResult<T> async)
    {
        this.async = async;
    }

    public boolean hasNext()
    {
        return async.hasNext();
    }

    public T next()
    {
        try { return async.next().get(); }
        catch (Exception ex) { throw new RuntimeException(ex); }
    }

    public void remove()
    {
        async.remove();
    }

    public boolean hasPrev()
    {
        return async.hasPrev();
    }

    public T prev()
    {
        try { return async.prev().get(); }
        catch (Exception ex) { throw new RuntimeException(ex); }
    }

    public T current()
    {
        try { return async.current().get(); }
        catch (Exception ex) { throw new RuntimeException(ex); }        
    }

    public void close()
    {
        async.close();
    }

    public boolean isOrdered()
    {
        return async.isOrdered();
    }
}