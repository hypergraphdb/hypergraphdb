package org.hypergraphdb.util;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 * A wrapper of an underlying executor service implementation that ensures
 * a transaction context is inherited from the calling thread when a task
 * is submitted. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class HGExecutorService implements ExecutorService
{
    private ExecutorService impl;
    
    public void execute(Runnable command)
    {
        impl.execute(command);
    }

    public void shutdown()
    {
        impl.shutdown();
    }

    public List<Runnable> shutdownNow()
    {
        return impl.shutdownNow();
    }

    public boolean isShutdown()
    {
        return impl.isShutdown();
    }

    public boolean isTerminated()
    {
        return impl.isTerminated();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return impl.awaitTermination(timeout, unit);
    }

    public <T> Future<T> submit(Callable<T> task)
    {
        return impl.submit(task);
    }

    public <T> Future<T> submit(Runnable task, T result)
    {
        return impl.submit(task, result);
    }

    public Future<?> submit(Runnable task)
    {
        return impl.submit(task);
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
            throws InterruptedException
    {
        return impl.invokeAll(tasks);
    }

    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks,
                                         long timeout, TimeUnit unit)
            throws InterruptedException
    {
        return impl.invokeAll(tasks, timeout, unit);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
            throws InterruptedException, ExecutionException
    {
        return impl.invokeAny(tasks);
    }

    public <T> T invokeAny(Collection<? extends Callable<T>> tasks,
                           long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException
    {
        return impl.invokeAny(tasks, timeout, unit);
    }
}