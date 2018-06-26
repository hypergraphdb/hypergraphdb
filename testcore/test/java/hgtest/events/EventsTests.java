/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2018 Kobrix Software, Inc.  All rights reserved. 
 */
package hgtest.events;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.event.HGAtomDefinedEvent;
import org.hypergraphdb.event.HGAtomRefusedException;
import org.hypergraphdb.event.HGDefineProposeEvent;
import org.hypergraphdb.event.HGEvent;
import org.hypergraphdb.event.HGListener;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import hgtest.HGTestBase;

public class EventsTests extends HGTestBase
{
	private Map<Class<? extends HGEvent>, Collection<HGEvent>> events =
			new HashMap<Class<? extends HGEvent>, Collection<HGEvent>>();
	
	private HGListener collectingListener = new HGListener() {
		@Override
		public Result handle(HyperGraph graph, HGEvent event)
		{
			if (!events.containsKey(event.getClass()))
				events.put(event.getClass(), new ArrayList<HGEvent>());
			events.get(event.getClass()).add(event);
			return Result.ok;
		}
	};
	
	private void assertEventType(Class<? extends HGEvent> eventType)
	{
		Assert.assertTrue(events.containsKey(eventType));
	}
	
	@Before
	public void initEventTracker()
	{
		graph.getEventManager().addListener(HGDefineProposeEvent.class, collectingListener);
		graph.getEventManager().addListener(HGAtomDefinedEvent.class, collectingListener);
	}
	
	@After
	public void clearEventTracker()
	{
		events.clear();
		graph.getEventManager().removeListener(HGDefineProposeEvent.class, collectingListener);
		graph.getEventManager().removeListener(HGAtomDefinedEvent.class, collectingListener);
	}
	
    @Test
    public void defineProposeOk()
    {
        HGHandle atomh = graph.getHandleFactory().makeHandle();
        graph.define(atomh, "DefineProposeOk");
        assertEventType(HGDefineProposeEvent.class);
    }

    @Test(expected = HGAtomRefusedException.class)
    public void defineProposeRefused()
    {
        HGHandle atomh = graph.getHandleFactory().makeHandle();
        final String atom = "DefineProposeRefused";
        HGListener refusedListener = new HGListener() {
    		@Override
    		public Result handle(HyperGraph graph, HGEvent event)
    		{
    			HGDefineProposeEvent definedEvent = (HGDefineProposeEvent)event;
    			return (definedEvent.getAtom().equals(atom)) ?  Result.cancel : Result.ok;
    		}        	
        };
        graph.getEventManager().addListener(HGDefineProposeEvent.class, refusedListener);
        try 
        {
        	graph.define(atomh, atom);
        }
        finally
        {
        	graph.getEventManager().removeListener(HGDefineProposeEvent.class, refusedListener);
        	assertEventType(HGDefineProposeEvent.class);
        }
    }

    
    @Test
    public void definedOk()
    {
        HGHandle atomh = graph.getHandleFactory().makeHandle();
        graph.define(atomh, "DefinedOk");
        assertEventType(HGAtomDefinedEvent.class);
    }
    
}
