package org.hypergraphdb.util;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MappedFuture<From, To> implements Future<To>
{
    Future<From> future;
    Mapping<From,To> map;
    
    public MappedFuture(Future<From> future, Mapping<From,To> map)
    {
        this.future = future;
        this.map = map;
    }
    
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return future.cancel(mayInterruptIfRunning);
    }

    public boolean isCancelled()
    {
        return future.isCancelled();
    }

    public boolean isDone()
    {
        return future.isDone();
    }

    public To get() throws InterruptedException, ExecutionException
    {
        From x = future.get();
        return map.eval(x);
    }

    public To get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException
    {
        From x = future.get();
        return map.eval(x);
    }
}