package org.hypergraphdb.event;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.transaction.TxList;
import org.hypergraphdb.transaction.TxMap;
import org.hypergraphdb.transaction.VBox;

public class HGDefaultEventManager implements HGEventManager
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
	
	private void init()
	{
	    listenerMap = new TxMap<Class<?>, List<HGListener>>(graph.getTransactionManager(), 
                new HashMap<Class<?>, VBox<List<HGListener>>>());	    		
	}
	
	public HGDefaultEventManager() { }
	public HGDefaultEventManager(HyperGraph graph)
	{
	    this.graph = graph;
	    init();
	}
	
	public <T extends HGEvent> void addListener(Class<T> eventType, HGListener listener)
	{
		getListeners(eventType).add(listener);
	}
	
	public <T extends HGEvent> void removeListener(Class<T> eventType, HGListener listener)
	{
		getListeners(eventType).remove(listener);
	}
	
	public  HGListener.Result dispatch(HyperGraph graph, HGEvent event)
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
				switch (l.handle(graph, event)) 
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

	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
		init();
	}
	
	public HyperGraph getHyperGraph()
	{
		return this.graph;		
	}
}