/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HGGraphHolder;
import org.hypergraphdb.HyperGraph;

/**
 * <p>
 * A <code>HGEventManager</code> is bound to a single HyperGraph instance. It is responsible for holding
 * all event listeners and it performs event handling  via its <code>dispatch</code> method. Concrete
 * implementations can be configured with the {@link HGConfiguration} object when the database is 
 * opened. 
 * </p>
 * 
 * <p>
 * Event types are simply identified by their Java class objects. 
 * </p>
 * 
 * @author Borislav Iordanov
 */
public interface HGEventManager extends HGGraphHolder
{
	/**
	 * Register a new listener for a given event type.
	 * 
	 * @param <T>
	 * @param eventType
	 * @param listener
	 */
	<T extends HGEvent> void addListener(Class<T> eventType, HGListener listener);
	
	/**
	 * Remove a listener registered for the particular type.
	 * @param <T>
	 * @param eventType
	 * @param listener
	 */
	<T extends HGEvent> void removeListener(Class<T> eventType, HGListener listener);
	
	/**
	 * Removal all event listeners for all event types.
	 */
	void clear();
	
	/**
	 * <p>
	 * Dispatch an event to all listeners registered for its type. All listeners
	 * are invoked in the order in which they were registered.  If a listener returns
	 * a {@link HGListener.Result.cancel} event, the dispatch process is interrupted and
	 * the cancellation is passed on to the caller of the dispatch method without invoking
	 * any further listeners.
	 * </p>
	 * 
	 * <p>
	 * Because event types can be organized in hierarchy, rooted at {@link HGEvent}, a dispatcher
	 * must call listener registered for the particular event class and also all its superclasses up 
	 * to the <code>HGEvent</code> type itself.
	 * </p>
	 * 
	 * @param graph
	 * @param event
	 * @return
	 */
	HGListener.Result dispatch(HyperGraph graph, HGEvent event);	
}