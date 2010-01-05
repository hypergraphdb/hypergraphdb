/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.event;

import org.hypergraphdb.HyperGraph;

/**
 * <p>
 * A <code>HGListener</code> must be implemented in order to be receive notifications
 * about HyperGraph events. Event listeners implement the 
 * </p> 
 */
public interface HGListener 
{
	public static enum Result
	{
		/**
		 * This result indicates that processing should continue normally.
		 */
		ok,
		/**
		 * This result indicates that processing should be cancelled. When a listener
		 * returns a <code>cancel</code> result, event processing is interrupted and
		 * the calling HyperGraph instance is notified. The HyperGraph instance may
		 * or may not honor the cancellation depending on the particular event type.
		 * This per-event-type behavior is document in each event class. 
		 */
		cancel;
		
		public String toString() 
		{ 
			if (this == ok) 
				return "HGListenerResult(OK)"; 
			else
				return "HGListenerResult(CANCEL)";
		}
	};
	
	Result handle(HyperGraph graph, HGEvent event);
}
