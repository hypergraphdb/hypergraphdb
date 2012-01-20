/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer;

import java.util.HashSet;
import java.util.Set;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGStore;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.event.HGAtomAddedEvent;
import org.hypergraphdb.event.HGAtomRemovedEvent;
import org.hypergraphdb.event.HGAtomReplacedEvent;
import org.hypergraphdb.event.HGEvent;
import org.hypergraphdb.event.HGListener;
import org.hypergraphdb.peer.log.Log;
import org.hypergraphdb.peer.replication.RememberTaskClient;
import org.hypergraphdb.storage.StorageGraph;

/**
 * @author ciprian.costa
 *
 *  Handles storage in the context of replication - manages log
 */
public class StorageService
{
	public enum Operation {Create, Update, Remove, Copy};

	private HyperGraph graph;
	private HyperGraph logGraph;

	Set<HGHandle> ownAddedHandles = new HashSet<HGHandle>();
	Set<HGHandle> ownUpdatedHandles = new HashSet<HGHandle>();
	Set<HGHandle> ownRemovedHandles = new HashSet<HGHandle>();

	private HyperGraphPeer thisPeer;
	private Log log;
	
	private boolean autoSkip = false;
	
	public StorageService(HyperGraphPeer thisPeer)
	{
	    this.thisPeer = thisPeer;
		this.graph = thisPeer.getGraph();
//		this.logGraph = thisPeer.getTempDb();
		this.log = thisPeer.getLog();
		
/*		graph.getEventManager().addListener(HGAtomAddedEvent.class, new AtomAddedListener());
		graph.getEventManager().addListener(HGAtomRemovedEvent.class, new AtomRemovedListener());
		graph.getEventManager().addListener(HGAtomReplacedEvent.class, new AtomReplacedListener()); */
	}
	
	public HGHandle storeSubgraph(StorageGraph subGraph, HGStore store)
	{	    
		SubgraphManager.store(subGraph, store);
		return subGraph.getRoots().iterator().next();
	}
	
	public HGHandle addSubgraph(StorageGraph subgraph)
	{
		//TODO remake to add directly to store and INDEX
		HGStore store = logGraph.getStore();
		HGHandle handle = storeSubgraph(subgraph, store);
		Object value = logGraph.get(handle);
		logGraph.remove(handle, false);
		
		
		ownAddedHandles.add(handle);	
		graph.define((HGPersistentHandle)handle, value);
		
		return handle;
		
	}

	public HGHandle updateSubgraph(StorageGraph subgraph)
	{
		HGStore store = logGraph.getStore();
		HGHandle handle = storeSubgraph(subgraph, store);
		Object value = logGraph.get(handle);
		logGraph.remove(handle, false);

		ownUpdatedHandles.add(handle);	
		graph.replace((HGPersistentHandle)handle, value);
		
		return handle;
	}


	public HGHandle addOrReplaceSubgraph(StorageGraph subgraph)
	{
		HGPersistentHandle handle = (HGPersistentHandle)subgraph.getRoots().iterator().next();
		
		if (graph.getStore().containsLink(handle))
		{
			updateSubgraph(subgraph);
		}else{
			addSubgraph(subgraph);
		}

		return handle;
	}

	public void remove(HGHandle handle)
	{
		ownRemovedHandles.add(handle);
		
		graph.remove(handle);
			
	}
	
	private class AtomAddedListener implements HGListener
	{

		public Result handle(HyperGraph hg, HGEvent event)
		{
			//someone added an object - get it and propagate (unless it is added by us) ...
			HGAtomAddedEvent addedEvent = (HGAtomAddedEvent)event;
			HGHandle handle = addedEvent.getAtomHandle();
			if (autoSkip || ownAddedHandles.contains(handle))
			{
				//we added it ... just skip
				System.out.println("Own add detected: " + handle);
				ownAddedHandles.remove(handle);
			}else{
				//someone else added ... propagate ... 
				System.out.println("Add to propagate: " + handle);

				RememberTaskClient client = new RememberTaskClient(thisPeer, hg.get(handle), log, hg, hg.getPersistentHandle(handle), Operation.Create);
				client.run();
			}
			
			return Result.ok;
		}
		
	}

	private class AtomRemovedListener implements HGListener
	{
		public Result handle(HyperGraph hg, HGEvent event)
		{
			HGAtomRemovedEvent removedEvent = (HGAtomRemovedEvent)event;
			HGHandle handle =  removedEvent.getAtomHandle();
			
			if (autoSkip || ownRemovedHandles.contains(handle))
			{
				System.out.println("own remove detected: " + handle);
				ownRemovedHandles.remove(handle);
			}else{
				System.out.println("Remove to propagate: " + handle);
				
				RememberTaskClient client = new RememberTaskClient(thisPeer, null, log, hg, hg.getPersistentHandle(handle), Operation.Remove);
				client.run();
			}
			
			return Result.ok;
		}
	}
	private class AtomReplacedListener implements HGListener
	{
		public Result handle(HyperGraph hg, HGEvent event)
		{
			HGAtomReplacedEvent replacedEvent = (HGAtomReplacedEvent)event;
			HGHandle handle =  replacedEvent.getAtomHandle();
			
			if (autoSkip || ownUpdatedHandles.contains(handle))
			{
				System.out.println("Own replace detected: " + handle);
				ownUpdatedHandles.remove(handle);
			}else{
				System.out.println("Replace to propagate: " + handle);
				
				RememberTaskClient client = new RememberTaskClient(thisPeer, hg.get(handle), log, hg, hg.getPersistentHandle(handle), Operation.Update);
				client.run();
			}
			
			return Result.ok;
		}
		
	}
	public void registerType(HGPersistentHandle handle, Class<?> clazz)
	{
		if ((graph!= null) && (graph.getStore().getLink(handle) == null))
		{
			autoSkip = true;
			graph.getTypeSystem().defineTypeAtom(handle, clazz);
			autoSkip = false;
		}
		
		if(logGraph.getStore().getLink(handle) == null)
		{
			logGraph.getTypeSystem().defineTypeAtom(handle, clazz);
		}
		
	}




}
