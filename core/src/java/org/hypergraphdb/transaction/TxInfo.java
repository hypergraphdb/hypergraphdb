/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A TxMonitoringRunner runs a HGDB transaction while monitoring 
 * and notifying a a tx monitor about success/failure, retries and
 * time to completion.
 */
public class TxInfo
{
	private String name;
	private long startTime; 
	private long endTime = 0;
	private int retryCount = 0;
	private long transactionNumber = 0;
	private Throwable failureException = null;
	private HashMap<String, Object> attributes = new HashMap<String, Object>();
	private HashSet<String> conflicting = new HashSet<String>(); // names of transactions which caused this one to retry
	
	public TxInfo(String name, long transactionNumber)
	{
		this.name = name;
		this.transactionNumber = transactionNumber;
		this.startTime = System.currentTimeMillis();
	}
	
	public TxInfo retried()
	{
		this.endTime = 0;
		this.failureException = null;
		retryCount++;
		return this;
	}
	
	public TxInfo succeeded()
	{
		this.endTime = System.currentTimeMillis();
		return this;
	}
	
	public TxInfo failed(Throwable failureException)
	{
		this.failureException = failureException;
		this.endTime = System.currentTimeMillis();
		return this;
	}
	
	public boolean isFinished()
	{
		return endTime > 0;
	}
	
	public long number()
	{
		return this.transactionNumber;
	}
	
	public String name()
	{
		return this.name;
	}	
	
	public long completionTime()
	{
		return this.endTime - this.startTime;
	}
	
	public long startTime()
	{
		return this.startTime;
	}
	
	public long endTime()
	{
		return this.endTime;
	}
	
	public Throwable failedWith()
	{
		return this.failureException;
	}
	
	public Set<String> conflicting()
	{
		return this.conflicting;
	}
	
	/**
	 * Return the number of time the transaction was retried, or 0 if it 
	 * completed the first time.
	 */
	public int retryCount()
	{
		return this.retryCount;
	}
		
	public TxInfo attr(String name, Object value)
	{
		this.attributes.put(name, value);
		return this;
	}
	
	@SuppressWarnings("unchecked")
	public <V> V attr(String name)
	{
		return (V)this.attributes.get(name);
	}
	
	public boolean has(String name)
	{
		return this.attributes.containsKey(name);
	}
	
	public boolean is(String name, Object value)
	{
		return this.has(name) && attr(name).equals(value);
	}	
	
	public Map<String, Object> attributes()
	{
		return this.attributes;
	}
}
