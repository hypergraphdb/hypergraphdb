package org.hypergraphdb.conv.types;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.hypergraphdb.HGException;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HGLink;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGQuery;
import org.hypergraphdb.HGSearchResult;
import org.hypergraphdb.HGTypeSystem;
import org.hypergraphdb.HGValueLink;
import org.hypergraphdb.annotation.AtomReference;
import org.hypergraphdb.atom.AtomProjection;
import org.hypergraphdb.atom.HGAtomRef;
import org.hypergraphdb.atom.HGRelType;
import org.hypergraphdb.conv.Converter;
import org.hypergraphdb.conv.DefaultConverter;
import org.hypergraphdb.conv.MetaData;
import org.hypergraphdb.conv.Converter.AddOnType;
import org.hypergraphdb.query.AtomValueCondition;
import org.hypergraphdb.type.AtomRefType;
import org.hypergraphdb.type.BonesOfBeans;
import org.hypergraphdb.type.HGAtomType;
import org.hypergraphdb.type.Record;
import org.hypergraphdb.type.RecordType;
import org.hypergraphdb.type.Slot;
import org.hypergraphdb.type.TypeUtils;

public class SwingType extends RecordType
{
	private Class<?> javaClass;
	private HGHandle ctrHandle;
	private HGHandle addOnsHandle;
	protected HashMap<String, HGHandle> slotHandles = new HashMap<String, HGHandle>();


	public SwingType(Class<?> javaClass)
	{
		this.javaClass = javaClass;
	}

	public void init(HGHandle typeHandle)
	{
		this.thisHandle = typeHandle;
		//System.out.println("SwingType: " + javaClass.getName());
		Converter c = MetaData.getConverter(javaClass);
		createSlots(c);
		ctrHandle = createCtrLink(c);
		addOnsHandle = createAddonsLink(c);
	}

	public Class<?> getJavaClass()
	{
		return javaClass;
	}

	public void release(HGPersistentHandle handle)
	{
		graph.getStore().removeLink(handle);
	}

	private HGHandle createCtrLink(Converter cin)
	{
		if (!(cin instanceof DefaultConverter))
			return HGHandleFactory.nullHandle();
		DefaultConverter c = (DefaultConverter) cin;
		if (c.getCtr() == null && c.getFactoryCtr() == null)
			return HGHandleFactory.nullHandle();
		String[] args = c.getCtrArgs();
		Class<?>[] types = c.getCtrTypes();
		if (c.getFactoryCtr() != null)
		{
			HGHandle[] targets = new HGHandle[args.length + 2];
			targets[0] = graph.add(c.getFactoryCtr().getDeclaringClass());
			targets[1] = graph.add(c.getFactoryCtr().getName());
			for (int i = 0; i < types.length; i++)
			{
				HGHandle[] r = new HGHandle[] { slotHandles.get(args[i]) };
				targets[i + 2] = graph.add(new HGValueLink(types[i], r));
			}
			return graph.add(new FactoryConstructorLink(targets));
		}
		HGHandle[] targets = new HGHandle[args.length];
		for (int i = 0; i < types.length; i++)
		{
			HGHandle[] r = new HGHandle[] { slotHandles.get(args[i]) };
			//if(targets[i] == null)
			//	System.err.println("CTRLink: " + args[i] + ":" + javaClass);
			if(types[i] == null)
				System.err.println("CTRLink - NULL type: " + args[i] + ":" + javaClass);
			targets[i] = graph.add(new HGValueLink(types[i], r));
			
		}
		return graph.add(new ConstructorLink(targets));
	}
	
	protected void createSlots(Converter c)
	{
		Map<String, Class<?>> slots = c.getSlots();
		System.out.println("SwingType: " + javaClass + ":" + slots.size() + ":" + 
				((DefaultConverter)c).getType() + ":" + graph);
		
		HGTypeSystem typeSystem = graph.getTypeSystem();
		HGHandle slotType = typeSystem.getTypeHandle(Slot.class);
		for (String s : slots.keySet())
		{
			Slot slot = new Slot();
			slot.setLabel(s); 
			//System.out.println("Slot: " + slot.getLabel());
			Class<?> propType = slots.get(s);
			if (propType == null)
				System.err.println("NULL Class for " + s + " in " + javaClass);
			if (propType.isPrimitive())
				propType = BonesOfBeans.wrapperEquivalentOf(propType);
			HGHandle valueTypeHandle = typeSystem.getTypeHandle(propType);
			slot.setValueType(valueTypeHandle);
			AtomValueCondition cond = new AtomValueCondition(slot);
			HGQuery query = HGQuery.make(typeSystem.getHyperGraph(), cond);
			HGSearchResult<HGHandle> res = null;
			HGHandle slotHandle;
			try
			{
				res = query.execute();
				if (!res.hasNext())
				{
					slotHandle = typeSystem.getHyperGraph().add(slot, slotType);
				} else
				// the Slot is in the DB, but not in cache so put it
				// //here
				{
					slotHandle = res.next();
					slot = (Slot) typeSystem.getHyperGraph().get(slotHandle);
					if(slot == null) 
						System.err.println("NULL AddOnSlot in SwingType: " + javaClass);
				}
			}
			catch (Throwable t)
			{
				throw new HGException(t);
			}
			finally
			{
				if (res != null) res.close();
			}
			
			addSlot(slotHandle);
			HGAtomRef.Mode refMode = getReferenceMode(javaClass, slot.getLabel());
			if (refMode != null)
				typeSystem.getHyperGraph().add(
						new AtomProjection(thisHandle, slotHandle, refMode));
		}
	}

	protected HGHandle createAddonsLink(Converter cin)
	{
		if (!(cin instanceof DefaultConverter))
			return HGHandleFactory.nullHandle();
		DefaultConverter c = (DefaultConverter) cin;
		Set<AddOnType> set = c.getAllAddOnFields();
		if (set == null) return HGHandleFactory.nullHandle();
		HGHandle[] targets = new HGHandle[set.size()];
		int i = 0;
		for (AddOnType a : set)
		{
			String[] args = a.getArgs();
			HGHandle[] t = new HGHandle[args.length];
			for (int j = 0; j < args.length; j++){
				t[j] = slotHandles.get(args[j]);
				System.out.println("AddOnSlots: " + graph.get(t[j]));
			}
			HGLink link = new HGRelType(a.getName(), t);
			System.out.println("ADDONLINK: " + javaClass + ":" + a.getName()
					+ ":" + args[0] + ":" + args.length);
			targets[i] = graph.add(link);
			i++;
		}
		return graph.add(new AddOnLink(targets));
	}

	public HGHandle getAddOnsHandle()
	{
		return addOnsHandle;
	}

	public HGHandle getCtrHandle()
	{
		return ctrHandle;
	}

	public void setAddOnsHandle(HGHandle addOnsHandle)
	{
		this.addOnsHandle = addOnsHandle;
	}

	public void setCtrHandle(HGHandle ctrHandle)
	{
		this.ctrHandle = ctrHandle;
	}

	public void addSlot(HGHandle slot)
	{
		if (!slots.contains(slot)) 
		{
			slots.add(slot);
			Slot s = (Slot) graph.get(slot);
			if( s!= null)
			  slotHandles.put(s.getLabel(), slot);
		}
	}

	public void remove(HGHandle slot)
	{
		int i = slots.indexOf(slot);
		if(i >= 0 && i < slots.size())
			removeAt(i);
	}

	public void removeAt(int i)
	{
		Slot s = (Slot) graph.get(slots.get(i));
		slots.remove(i);
		slotHandles.remove(s);
	}


	private HGAtomRef.Mode getReferenceMode(Class<?> javaClass, String name)
	{
		//
		// Retrieve or recursively create a new type for the nested
		// bean.
		//
		try
		{
			Field field = javaClass.getDeclaredField(name);
			AtomReference ann = (AtomReference) field
					.getAnnotation(AtomReference.class);
			if (ann == null) return null;
			String s = ann.value();
			if ("hard".equals(s))
				return HGAtomRef.Mode.hard;
			else if ("symbolic".equals(s))
				return HGAtomRef.Mode.symbolic;
			else if ("floating".equals(s))
				return HGAtomRef.Mode.floating;
			else
				throw new HGException(
						"Wrong annotation value '"
								+ s
								+ "' for field '"
								+ field.getName()
								+ "' of class '"
								+ javaClass.getName()
								+ "', must be one of \"hard\", \"symbolic\" or \"floating\".");
		}
		catch (NoSuchFieldException ex)
		{
			// Perhaps issue a warning here if people are misspelling
			// unintentionally? Proper spelling is only useful for
			// annotation, so a warning/error should be really issued if
			// we find an annotation for a field that we can't make
			// use of?
			return null;
		}
	}
	
	 public HGPersistentHandle store(Object instance)
	    {
	        HGPersistentHandle handle = TypeUtils.getNewHandleFor(graph, instance);
	        if (! (instance instanceof Record))
	            throw new HGException("RecordType.store: object is not of type Record.");
	        Record record = (Record)instance;
	        HGPersistentHandle [] layout = new HGPersistentHandle[slots.size() * 2];
	        for (int i = 0; i < slots.size(); i++)
	        {     	
	        	HGHandle slotHandle = getAt(i);
	        	Slot slot = (Slot)graph.get(slotHandle);
	            Object value = record.get(slot);            
	            if (value == null)
	            {
	            	layout[2*i] = graph.getPersistentHandle(slot.getValueType());
	            	layout[2*i + 1] = HGHandleFactory.nullHandle();
	            }
	            else
	            {
		        	HGAtomRef.Mode refMode = getReferenceMode(slotHandle);        	
		        	if (refMode == null)
		        	{
		                HGHandle actualTypeHandle = graph.getTypeSystem().getTypeHandle(value.getClass());
		                if (actualTypeHandle == null)
		                	actualTypeHandle = slot.getValueType();
		                HGAtomType type = graph.getTypeSystem().getType(actualTypeHandle);                
		                layout[2*i] = graph.getPersistentHandle(actualTypeHandle);
		                layout[2*i + 1] = TypeUtils.storeValue(graph, value, type);
		        	}
		        	else
		        	{
		                layout[2*i] = graph.getPersistentHandle(slot.getValueType());
		                if (value instanceof HGAtomRef)
		                {
			        		AtomRefType refType = (AtomRefType)graph.get(AtomRefType.HGHANDLE);
			                layout[2*i + 1] = refType.store((HGAtomRef)value);
		                }
		                else
		                	throw new HGException("Slot " + slot.getLabel() + 
		                						  " should have an atom reference for record " + 
		                						  graph.getHandle(this));
		        	}
	            }
	        }
	        graph.getStore().store(handle, layout);
	        return handle;
	    }
	 
	 public synchronized HGAtomRef.Mode getReferenceMode(HGHandle slot)
	 {
		 if(thisHandle == null)	{
			 System.out.println("SwingTypeStore-getReferenceMode: " + slot + ":" + thisHandle);
			 Thread.currentThread().dumpStack();
		 }
         return super.getReferenceMode(slot);
	 }
}
