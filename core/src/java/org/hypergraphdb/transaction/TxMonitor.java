/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.hypergraphdb.util.HGUtils;
import org.hypergraphdb.util.Mapping;

class TxMonitor implements TransactionMonitor
{
	public static class Info
	{
		public long id = 0;
		public String threadName = null;
		public String beginTrace = null;
		public String endTrace = null;
	}
	
	Map<Long, TxInfo> txMap = 
			Collections.synchronizedMap(new HashMap<Long, TxInfo>());
			
	private AtomicLong operationNumber = new AtomicLong(0);
    volatile boolean enabled = false;
	private HGTransactionManager manager;
	
	static class MonitorFilter implements Mapping<TxInfo, Boolean> 
	{
		HashMap<String, Object> params = new HashMap<String, Object>();
		
		public MonitorFilter(Object...params)
		{
			for (int i = 0; i < params.length; i += 2)
				this.params.put(params[i].toString(), params[i+1]);
		}
		
		@Override
		public Boolean eval(TxInfo x)
		{
			for (Map.Entry<String, Object> e : params.entrySet())
				if (!x.is(e.getKey(), e.getValue()))
					return false;
			return true;
		}
	}
	
	Set<String> activeNames(TxInfo except)
	{
		HashSet<String> S = new HashSet<String>();
		for (TxInfo info : txMap.values())
			if (!info.isFinished() && info.number() != except.number())
				S.add(info.name() + " - " + info.number());
		return S;
	}
	
	TxMonitor(HGTransactionManager manager)
	{
		this.manager = manager;
	}

	public boolean enabled()
	{
		return this.enabled;
	}
	
	@Override
	public TransactionMonitor enable()
	{
		this.enabled = true;
		return this;
	}

	@Override
	public TransactionMonitor disable()
	{
		this.enabled = false;
		return this;
	}

	@Override
	public TxInfo tx()
	{
		if (manager.getContext().getCurrent() != null)
		{
			return (TxInfo)manager.getContext().getCurrent().getAttribute(hgdbmonitorTxInfoKey); 
		}
		else
			return null;
	}

	public Set<TxInfo> lookup(Object... params)
	{
		HashSet<TxInfo> S = new HashSet<TxInfo>();
		MonitorFilter filter = new MonitorFilter(params);
		for (TxInfo tx : txMap.values())
			if (filter.eval(tx))
				S.add(tx);
		return S;
	}	
	
	public TxInfo startTransaction(String name)
	{
		long txNumber = operationNumber.incrementAndGet();
		TxInfo txinfo = new TxInfo(name, txNumber);
		this.txMap.put(txNumber, txinfo);		
		return txinfo;
	}
	
	public <V> V transact(String name, Callable<V> transaction, HGTransactionConfig config)
	{
		TxInfo txinfo = startTransaction(name);
		while (true)
		{		    
			manager.beginTransaction(config);
			manager.getContext().getCurrent().setAttribute(hgdbmonitorTxInfoKey, txinfo);
			V result;
			try
			{
				result = transaction.call();
			}
			catch (HGUserAbortException ex)
			{
				try { manager.endTransaction(false); }
				catch (HGTransactionException tex) { tex.printStackTrace(System.err); }
				txinfo.failed(ex);
				return null;
			}
			catch (Throwable t)
			{
				try { manager.endTransaction(false); }
				catch (HGTransactionException tex) { tex.printStackTrace(System.err); }
				if (HGUtils.getRootCause(t) instanceof TransactionIsReadonlyException && 
				    config.isWriteUpgradable())
				{
				    config = HGTransactionConfig.DEFAULT;
				}
				else
				{
					txinfo.failed(t);
					manager.handleTxException(t); // will re-throw if we can't retry the transaction
					txinfo.retried().conflicting().addAll(activeNames(txinfo));
				}
				continue;
			}
			try
			{
				manager.endTransaction(true);		
				txinfo.succeeded();
				return result;
			}  
			catch (Throwable t)
			{
                if (HGUtils.getRootCause(t) instanceof TransactionIsReadonlyException && 
                        config.isWriteUpgradable())
                {
                    config = HGTransactionConfig.DEFAULT;
                }
                else
                {
                	txinfo.failed(t);
                    manager.handleTxException(t); // will re-throw if we can't retry the transaction
                    txinfo.retried().conflicting().addAll(activeNames(txinfo));                    
                }
			}
		}		
	}
	
	public TransactionMonitor clear()
	{
		txMap.clear();
		return this;
	}
}
