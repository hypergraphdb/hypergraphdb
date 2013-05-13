/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * <p>
 * An <code>AggregateFuture</code> encapsulates several <code>Future</code>
 * into a single one. The result of an aggregate future is the list of 
 * results of all its components in the order in which they were added.
 * </p>
 */
public class AggregateFuture<T> implements Future<List<T>>
{	
	private List<Future<T>> components = new ArrayList<Future<T>>();

    /**
     * <p>
     * Construct from a set of futures which is copied
     * internally.
     * </p>
     */
    public AggregateFuture(Future<T>...futures)
    {
        for (Future<T> f : futures)
            components.add(f);
    }
    
	/**
	 * <p>
	 * Construct from a non-null list of components which is copied
	 * internally.
	 * </p>
	 */
	public AggregateFuture(List<Future<T>> components)
	{
		this.components.addAll(components);
	}
	
	/**
	 * <p>Canceling an aggregate future succeeds only if canceling
	 * all of its components succeeds. Note that during the process
	 * some components may be canceled while others not which would
	 * lead to an inconsistent state.</p>
	 */
	public boolean cancel(boolean mayInterruptIfRunning)
	{
		for (Future<T> f : components)
			if (!f.cancel(mayInterruptIfRunning))
				return false;
		return true;
	}

	public List<T> get() throws InterruptedException, ExecutionException
	{
		List<T> value = new ArrayList<T>();
		for (Future<T> f : components)
			value.add(f.get());
		return value;
	}

	public List<T> get(long timeout, TimeUnit unit) 
		throws InterruptedException, ExecutionException, TimeoutException
	{
		List<T> value = new ArrayList<T>();
		for (Future<T> f : components)
			value.add(f.get(timeout, unit));
		return value;
	}

	/**
	 * An aggregate is canceled iff at least one of its components is canceled.
	 */
	public boolean isCancelled()
	{
		for (Future<T> f : components)
			if (f.isCancelled())
				return true;
		return false;
	}

	/**
	 * An aggregate is done iff all of its components are done.
	 */
	public boolean isDone()
	{
		for (Future<T> f : components)
			if (!f.isDone())
				return false;
		return true;
	}
}
