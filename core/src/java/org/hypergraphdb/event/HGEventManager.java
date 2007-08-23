/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.event;

import java.util.ArrayList;
import java.util.HashMap;

import org.hypergraphdb.HyperGraph;

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
	private HashMap<Class, ArrayList<HGListener>> listenerMap = new HashMap<Class, ArrayList<HGListener>>();
	
	public <T extends HGEvent> void addListener(Class<T> eventType, HGListener<T> listener)
	{
		ArrayList<HGListener> listeners = listenerMap.get(eventType);
		if (listeners == null)
		{
			listeners = new ArrayList<HGListener>();
			listenerMap.put(eventType, listeners);			
		}
		listeners.add(listener);
	}
	
	public <T extends HGEvent> void removeListener(Class<T> eventType, HGListener<T> listener)
	{
		ArrayList<HGListener> listeners = listenerMap.get(eventType);
		if (listeners == null)
			return;
		else
			listeners.remove(listener);
	}
	
	public <T extends HGEvent> HGListener.Result dispatch(HyperGraph hg, T event)
	{
		for (Class clazz = event.getClass(); HGEvent.class.isAssignableFrom(clazz); clazz = clazz.getSuperclass())
		{
			ArrayList<HGListener> listeners = listenerMap.get(clazz);
			if (listeners == null)
				continue;
			for (HGListener l : listeners)
				// type safety warning OK, we are explicitely passing a correctly typed event.
				switch (l.handle(hg, event)) 
				{
					case ok: continue;
					case cancel: return HGListener.Result.cancel;
				}
		}		
		return HGListener.Result.ok;
	}
	
	public void clear()
	{
		listenerMap.clear();
	}
}