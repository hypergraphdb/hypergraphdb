/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

/**
 * <p>
 * This is a simple bean-like atom that can be used to register listeners that
 * will be automatically loaded when a HyperGraph is open. To configure such an
 * auto-loaded listeners, instantiate a <code>HGListenerAtom</code>, set the
 * event type and listener class properties and add it to a HyperGraph instance. The listener
 * will be automatically registered with the event manager the next time this HyperGraph 
 * is opened. If the
 * listener should become effective right away, you will need to register it with
 * the event manager separately.   
 * </p>
 * 
 * @author boris
 *
 */
public class HGListenerAtom 
{
	private String listenerClassName;
	private String eventClassName;
	
	public HGListenerAtom()
	{		
	}
	
	public HGListenerAtom(String eventClassName, String listenerClassName)
	{
		this.eventClassName = eventClassName;
		this.listenerClassName = listenerClassName;
	}

	public String getEventClassName() {
		return eventClassName;
	}

	public void setEventClassName(String eventClassName) {
		this.eventClassName = eventClassName;
	}

	public String getListenerClassName() {
		return listenerClassName;
	}

	public void setListenerClassName(String listenerClassName) {
		this.listenerClassName = listenerClassName;
	}	
}
