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
import java.util.Map;

public class TxMonitor
{
	public static class Info
	{
		public long id = 0;
		public String threadName = null;
		public String beginTrace = null;
		public String endTrace = null;
	}
	
	public Map<Long, Info> txMap = Collections.synchronizedMap(new HashMap<Long, Info>());
	
	public void transactionCreated(HGTransaction tx)
	{
		try
		{
			Info txInfo = new Info();
			txInfo.id = tx.getNumber();
			txInfo.threadName = Thread.currentThread().getName();
			StringBuffer b = new StringBuffer();
			for (StackTraceElement el : Thread.currentThread().getStackTrace())
				{ b.append(el.toString()); b.append("\n"); }
			txInfo.beginTrace = b.toString();
			txMap.put(txInfo.id, txInfo);
		}
		catch (Throwable t) { t.printStackTrace(); }
	}
	
	public void transactionFinished(HGTransaction tx)
	{
		try
		{
			long id = tx.getNumber();
			Info txInfo = txMap.remove(id);
			if (txInfo == null)
			{
				// throw new NullPointerException("No transaction with ID " + id + " was recorded to start.");
//				System.err.println("WARN - no transaction with ID " + id + " in monitor.");
				// this is quite possible since several transactions with the same id can be initiated
				// concurrently and only one of them will succeed...
				return;
			}
			StringBuffer b = new StringBuffer();
			for (StackTraceElement el : Thread.currentThread().getStackTrace())
				{ b.append(el.toString()); b.append("\n"); }
			txInfo.endTrace = b.toString();
		}
		catch (Throwable t)
		{
			t.printStackTrace();
		}
	}	
}
