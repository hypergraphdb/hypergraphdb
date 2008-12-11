package org.hypergraphdb.peer;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.handle.UUIDPersistentHandle;
import org.hypergraphdb.util.Pair;

/**
 * @author Cipri Costa
 * Class that will expose a HGDB subgraph in an form that can be iterated. Used mainly for 
 * serializing atoms to a byte array and sending them as part of a message.
 */
public class Subgraph
{
	private HyperGraph graph = null;
	private HGPersistentHandle handle = null;
	private LinkedList<Pair<HGPersistentHandle, Object>> buffer = null;

	public Subgraph(HyperGraph graph, HGPersistentHandle handle)
	{
		this.graph = graph;
		this.handle = handle;
	}
	
	public Subgraph()
	{
		buffer = new LinkedList<Pair<HGPersistentHandle,Object>>();
	}

	public Iterator<Pair<HGPersistentHandle, Object>> iterator()
	{
		//check this subgraph is actually connected to a graph
		if (handle != null)	return new SubgraphIterator();
		else if (buffer != null) return buffer.iterator();
		else return null;
	}
	
	public class SubgraphIterator implements Iterator<Pair<HGPersistentHandle, Object>>
	{
		LinkedList<HGPersistentHandle> remaining = new LinkedList<HGPersistentHandle>();
		HashSet<HGPersistentHandle> visited = new HashSet<HGPersistentHandle>();
		HGStore store;
		
		public SubgraphIterator()
		{
			store = graph.getStore();
			remaining.addLast(handle);
			
			//TODO some UUIDs should not be visited?
			visited.add(UUIDPersistentHandle.UUID_NULL_HANDLE);
		}
		
		public boolean hasNext()
		{
			return !remaining.isEmpty();
		}

		public Pair<HGPersistentHandle, Object> next()
		{
			Pair<HGPersistentHandle, Object> result = null;
			HGPersistentHandle h = remaining.removeFirst();
			HGPersistentHandle[] link = store.getLink(h);
		   
			if (link == null)
			{
				byte [] data = store.getData(h);
				//TODO throw exception for missing data
		        if (data != null) 
		        {
			        visited.add(h);
			        result = new Pair<HGPersistentHandle, Object>(h, data);
		        }
		   }
		   else
		   {
			   visited.add(h);
			   for (HGPersistentHandle x : link)
				   if (!visited.contains(x)) remaining.addLast(x);

			   result = new Pair<HGPersistentHandle, Object>(h, link);
		   }

			return result;
		}

		public void remove()
		{
			throw new UnsupportedOperationException();
		}
	}

	public void addToBuffer(HGPersistentHandle handle, Object value)
	{
		buffer.addLast(new Pair<HGPersistentHandle, Object>(handle, value));
	}

	public HGPersistentHandle getHandle()
	{
		if (handle != null) return handle;
		else if ((buffer == null) || (buffer.size() == 0)) return null;
		else return buffer.get(0).getFirst();
	}

	public void setHandle(HGPersistentHandle handle)
	{
		this.handle = handle;
	}

	
}
