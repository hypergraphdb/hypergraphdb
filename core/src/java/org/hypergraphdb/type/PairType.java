package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.IncidenceSetRef;
import org.hypergraphdb.LazyRef;
import org.hypergraphdb.util.Pair;

public class PairType extends HGAtomTypeBase
{

	public Object make(HGPersistentHandle handle,
			LazyRef<HGHandle[]> targetSet, IncidenceSetRef incidenceSet)
	{		
		HGPersistentHandle[] layout = graph.getStore().getLink(handle);
		Pair<?,?> result = (Pair<?,?>)TypeUtils.getValueFor(graph, handle);
		if (result != null)
			return result;
		Object first = null, second = null;
		if (!layout[0].equals(graph.getHandleFactory().nullHandle()))
		{
			HGAtomType type = graph.getTypeSystem().getType(layout[0]);
			first = TypeUtils.makeValue(graph, layout[1], type);			
		}
		if (!layout[2].equals(graph.getHandleFactory().nullHandle()))
		{
			HGAtomType type = graph.getTypeSystem().getType(layout[2]);
			//2012.01.24 hilpold BUGFIX old: first = TypeUtils.makeValue(graph, layout[3], type);			
			second = TypeUtils.makeValue(graph, layout[3], type);
		}		
		result = new Pair<Object,Object>(first, second);
		return result;
	}

	public void release(HGPersistentHandle handle)
	{
		HGPersistentHandle[] layout = graph.getStore().getLink(handle);
		for (int i = 0; i < layout.length; i += 2)
		{
			HGPersistentHandle typeHandle = layout[i];
			HGPersistentHandle valueHandle = layout[i + 1];
			if (typeHandle.equals(graph.getHandleFactory().nullHandle()))
			    continue;			
			if (!TypeUtils.isValueReleased(graph, valueHandle))
			{
			    HGAtomType type = graph.get(typeHandle);
				TypeUtils.releaseValue(graph, type, valueHandle);
				//2012.01.25 hilpold Bugfix removed: type.release(valueHandle);
			}
		}
		graph.getStore().removeLink(handle);		
	}

	public HGPersistentHandle store(Object instance)
	{
		Pair<?,?> p = (Pair<?,?>)instance;
		HGPersistentHandle result = TypeUtils.getNewHandleFor(graph, instance);
		HGPersistentHandle [] layout = new HGPersistentHandle[]
		                     { graph.getHandleFactory().nullHandle(),
							   graph.getHandleFactory().nullHandle(),
							   graph.getHandleFactory().nullHandle(),
							   graph.getHandleFactory().nullHandle()};
		if (p.getFirst() != null)		
		{
			layout[0] = graph.getPersistentHandle(graph.getTypeSystem().getTypeHandle(p.getFirst()));
			layout[1] = TypeUtils.storeValue(graph, p.getFirst(), (HGAtomType)graph.get(layout[0]));
		}
		if (p.getSecond() != null)		
		{
			layout[2] = graph.getPersistentHandle(graph.getTypeSystem().getTypeHandle(p.getSecond()));
			layout[3] = TypeUtils.storeValue(graph, p.getSecond(), (HGAtomType)graph.get(layout[2]));
		}
		graph.getStore().store(result, layout);
		return result;
	}
}
