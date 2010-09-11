/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.transaction.TxList;
import org.hypergraphdb.transaction.TxMap;
import org.hypergraphdb.transaction.VBox;

/**
 * <p>
 * A <code>HGEventManager</code> is bound to a single HyperGraph instance. It is responsible for holding
 * all event listeners and it performs event handling  via its <code>dispatch</code> method.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class HGEventManager 
{
    private HyperGraph graph;
	private Map<Class<?>, List<HGListener>> listenerMap = null;
	
	private List<HGListener> getListeners(Class<?> eventType)
	{
        List<HGListener> listeners = listenerMap.get(eventType);
        if (listeners == null)
        {
            listeners = new TxList<HGListener>(graph.getTransactionManager());
            listenerMap.put(eventType, listeners);          
        }
        return listeners;
	}
	
	public HGEventManager(HyperGraph graph)
	{
	    this.graph = graph;
	    listenerMap = new TxMap<Class<?>, List<HGListener>>(graph.getTransactionManager(), 
	                                                        new HashMap<Class<?>, VBox<List<HGListener>>>());	    
	}
	
	public <T extends HGEvent> void addListener(Class<T> eventType, HGListener listener)
	{
		getListeners(eventType).add(listener);
	}
	
	public <T extends HGEvent> void removeListener(Class<T> eventType, HGListener listener)
	{
		getListeners(eventType).remove(listener);
	}
	
	public  HGListener.Result dispatch(HyperGraph hg, HGEvent event)
	{
	    if (listenerMap.isEmpty()) // avoid looping through the class hierarchy cause it's expensive
	        return HGListener.Result.ok;
		for (Class<?> clazz = event.getClass(); clazz != null && HGEvent.class != clazz; clazz = clazz.getSuperclass())
		{
			List<HGListener> listeners = listenerMap.get(clazz);
			if (listeners == null)
				continue;
			for (HGListener l : listeners)
				// type safety warning OK, we are explicitly passing a correctly typed event.
				switch (l.handle(hg, event)) 
				{
					case ok: continue;
					case cancel: return HGListener.Result.cancel;
				}
		}
		// should we also invoke listener bound to HGEvent.class itself?
		return HGListener.Result.ok;
	}
	
	public void clear()
	{
		listenerMap.clear();
	}
}