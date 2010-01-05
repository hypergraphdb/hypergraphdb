/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.util;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 
 * <p>
 * An <code>CallbackFuture</code> offers the possibility to call back a registered
 * listener when it is completed. To avoid taking up a thread and waiting for
 * completion, this is accomplished by required the creator of the future
 * to invoke the <code>completed</code> method when the result this 
 * <code>Future</code> represents has been computed. 
 * </p>
 *
 * <p>
 * Only one listener can be registered for call back and it is of type
 * Mapping<CallbackFuture<T>, T>. That is, a mapping that will receive
 * the <code>CallbackFuture</code> itself as an argument and must return
 * the result of the <code>Future</code>. Typically the mapping will
 * just return the <code>CallbackFuture</code>'s own result by
 * calling the <code>getResult</code>, but it may choose to assign a different 
 * result. Note that blocking calls to <code>get</code> will not return
 * until the registered listener (if any) returns, so the listener
 * must rely on the provided <code>getResult</code> to obtain the 
 * <code>Future</code>'s result value. 
 * </p>
 *
 * <p>
 * This class may be extended to provide semantics for cancellation. In this
 * case the <code>cancel</code> method must be overridden and the 
 * <code>canceled</code> flag must be set to <code>true</code> if cancellation
 * was successful. 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class CallbackFuture<T> implements Future<T>
{
    private CountDownLatch latch;	
	private T result;
	private Mapping<CallbackFuture<T>, T> listener;
	protected volatile boolean canceled = false;
	
	public CallbackFuture()
	{
		latch = new CountDownLatch(1);
	}	
	public synchronized void setCompletionListener(Mapping<CallbackFuture<T>, T> listener)
	{
		this.listener = listener;
	}
	
	public synchronized void complete(T result)
	{
		if (isDone())
			throw new IllegalStateException("JobFuture completion attempted after it was done.");
		this.result = result;
		latch.countDown();
		if (listener != null)
			result = listener.eval(this);
	}
	
	public T getResult()
	{
		return result;
	}
	
	public boolean cancel(boolean mayInterruptIfRunning)
	{
		return false;
	}

	public T get() throws InterruptedException, ExecutionException
	{
		latch.await();
		return result;
	}

	public T get(long timeout, TimeUnit unit) throws InterruptedException,
			ExecutionException, TimeoutException
	{
		if (latch.await(timeout, unit))
			return result;
		else
			return null;
	}
	
	public boolean isCancelled()
	{
		return canceled;
	}

	public boolean isDone()
	{
		return latch.getCount() == 0 && !canceled;
	}
}
