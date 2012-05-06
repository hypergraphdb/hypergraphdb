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
	
	private List<HGListener> getListeners(Class<?> eventType, boolean addIfMissing)
	{
			if (listenerMap == null) {
				if (addIfMissing) {
					initListenerMap();
				}
				else {
					return null;
				}
			}
			
      List<HGListener> listeners = listenerMap.get(eventType);
      if (listeners == null && addIfMissing)
      {
          listeners = new TxList<HGListener>(graph.getTransactionManager());
          listenerMap.put(eventType, listeners);          
      }
      return listeners;
	}
	
	public HGDefaultEventManager() { }
	
	public HGDefaultEventManager(HyperGraph graph)
	{
	    this.graph = graph;
	}
	
	public void initListenerMap()
	{
	    listenerMap = new TxMap<Class<?>, List<HGListener>>(graph.getTransactionManager(), 
	                                                        new HashMap<Class<?>, VBox<List<HGListener>>>());	    
	}
	
	public <T extends HGEvent> void addListener(Class<T> eventType, HGListener listener)
	{
		getListeners(eventType, true).add(listener);
	}
	
	public <T extends HGEvent> void removeListener(Class<T> eventType, HGListener listener)
	{
		List<HGListener> listeners = getListeners(eventType, false);
		if (listeners != null) {
			listeners.remove(listener);
		}
	}
	
	public  HGListener.Result dispatch(HyperGraph hg, HGEvent event)
	{
    if (listenerMap == null || listenerMap.isEmpty()) // avoid looping through the class hierarchy cause it's expensive
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
		if (listenerMap != null) 
			listenerMap.clear();
	}

	public void setHyperGraph(HyperGraph graph)
	{
		this.graph = graph;
	}
	
	public HyperGraph getHyperGraph()
	{
		return this.graph;		
	}
}