package org.hypergraphdb.peer.log;

import java.util.HashMap;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.peer.StorageService;
import org.hypergraphdb.peer.Subgraph;
import org.hypergraphdb.peer.workflow.RememberTaskClient;

/**
 * @author ciprian.costa
 * Simple class that holds a log entry and its relations with the other peers.
 */
public class LogEntry implements Comparable<LogEntry>
{
	private Subgraph data;
	private HGPersistentHandle logEntryHandle;
	private HashMap<Object, Timestamp> lastTimestamps = new HashMap<Object, Timestamp>();
	Timestamp timestamp;
	StorageService.Operation operation;
	private HGPersistentHandle handle;
	
	public LogEntry(Object value, HyperGraph logDb, StorageService.Operation operation)
	{
		this(value, logDb, null, operation);
	}

	public LogEntry(Object value, HyperGraph logDb, HGPersistentHandle handle, StorageService.Operation operation)
	{
		this.handle = handle;
		
		if (operation != StorageService.Operation.Remove)
		{
			if (logDb.getStore().containsLink(handle))
			{
				logDb.replace(handle, value);
			}else{
				logDb.add(handle, value);
			}

			logEntryHandle = logDb.getPersistentHandle(handle);
			data = new Subgraph(logDb, logEntryHandle);
		}else
		{
			logEntryHandle = logDb.getPersistentHandle(handle);
		}

		this.operation = operation;
		
	}
	
	public LogEntry(HGHandle handle, HyperGraph logDb, Timestamp timestamp)
	{
		logEntryHandle = logDb.getPersistentHandle(handle);
		this.timestamp = timestamp;
		
		data = new Subgraph(logDb, logEntryHandle);
	}
	
	public Subgraph getData()
	{
		return data;
	}
	public void setData(Subgraph data)
	{
		this.data = data;
	}

	public HGPersistentHandle getLogEntryHandle()
	{
		return logEntryHandle;
	}

	public void setLogEntryHandle(HGPersistentHandle logEntryHandle)
	{
		this.logEntryHandle = logEntryHandle;
	}

	public void setTimestamp(Timestamp timestamp)
	{
		this.timestamp = timestamp;
	}
	public Timestamp getTimestamp()
	{
		return timestamp;
	}
	
	public void setLastTimestamp(Object targetId, Timestamp timestamp)
	{
		lastTimestamps.put(targetId, timestamp);
	}
	public Timestamp getLastTimestamp(Object targetId)
	{
		return lastTimestamps.get(targetId);
	}

	public StorageService.Operation getOperation()
	{
		return operation;
	}

	public void setOperation(StorageService.Operation operation)
	{
		this.operation = operation;
	}

	public HGPersistentHandle getHandle()
	{
		return handle;
	}

	public void setHandle(HGPersistentHandle handle)
	{
		this.handle = handle;
	}

	public int compareTo(LogEntry value)
	{
		if (value == null) return 1;
		else return timestamp.compareTo(value.getTimestamp());
	}
}
