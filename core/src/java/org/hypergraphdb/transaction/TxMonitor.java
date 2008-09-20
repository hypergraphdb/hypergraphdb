package org.hypergraphdb.transaction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class TxMonitor
{
	public static class Info
	{
		int id = 0;
		String threadName = null;
		String beginTrace = null;
		String endTrace = null;
	}
	
	public Map<Integer, Info> txMap = Collections.synchronizedMap(new HashMap<Integer, Info>());
	
	public void transactionCreated(HGTransaction tx)
	{
		try
		{
			Info txInfo = new Info();
			txInfo.id = ((TransactionBDBImpl)tx).getBDBTransaction().getId();
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
			int id;
			id = ((TransactionBDBImpl)tx).getBDBTransaction().getId();
			txMap.remove(id);
			if ( 1 == 1) return;
			Info txInfo = txMap.get(id);
			if (txInfo == null)
				throw new NullPointerException("No transaction with ID " + id + " was recorded to start.");
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