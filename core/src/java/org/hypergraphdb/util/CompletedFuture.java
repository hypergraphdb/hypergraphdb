/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
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
    private static final CompletedFuture<Object> null_instance = new CompletedFuture<Object>(null);
    
    @SuppressWarnings("unchecked")
    public static <T> CompletedFuture<T> getNull() { return (CompletedFuture<T>)null_instance; }
    
    V result;
    
    public CompletedFuture(V result)
    {
        this.result = result;
    }
    
    public boolean cancel(boolean mayInterruptIfRunning)
    {
        return false;
    }

    public V get() throws InterruptedException, ExecutionException
    {
        return result;
    }

    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            ExecutionException, TimeoutException
    {
        return result;
    }
    
    public boolean isCancelled()
    {
        return false;
    }

    
    public boolean isDone()
    {
        return true;
    }
}
