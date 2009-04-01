package org.hypergraphdb.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 * Represents a <code>Future</code> that's already completed. All methods
 * return immediately. The result of the future is provided in this class'
 * constructor.
 * </p>
 * 
 * @author Borislav Iordanov
 *
 * @param <V>
 */
public class CompletedFuture<V> implements Future<V>
{
    V result;
    
    public CompletedFuture(V result)
    {
        this.result = result;
    }
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return false;
    }

    @Override
    public V get() throws InterruptedException, ExecutionException
    {
        return result;
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException
    {
        return result;
    }

    @Override
    public boolean isCancelled()
    {
        return false;
    }

    @Override
    public boolean isDone()
    {
        return true;
    }
}