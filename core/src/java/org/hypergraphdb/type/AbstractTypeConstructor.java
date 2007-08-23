/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import java.util.Iterator;
import java.util.ArrayList;
import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.HGHandleFactory;
import org.hypergraphdb.HyperGraph;
import org.hypergraphdb.HGException;
import org.hypergraphdb.LazyRef;

/**
 * <p>This type is a predefined type constructor that manages abstract types
 * in the HyperGraph storage. It simply produces instances of <code>HGAbstractType</code>.
 * </p>
 *  
 * @author Borislav Iordanov
 */
public class AbstractTypeConstructor implements HGAtomType 
{
	private HyperGraph hg;
	
	public void setHyperGraph(HyperGraph hg) 
	{
		this.hg = hg;
	}

	public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet) 
	{
		if (targetSet != null && targetSet.deref().length > 0)
			throw new HGException("A HGAbstractType cannot be a link, " +
					"attempt to create an atom instance with a non-empty target set.");
		HGPersistentHandle [] layout = hg.getStore().getLink(handle);
		if (layout.length == 1)
			return new HGAbstractType();
		else
		{
			HGAbstractCompositeType type = new HGAbstractCompositeType();
			HGAtomType stringType = hg.getTypeSystem().getAtomType(String.class);			
			for (int i = 0; i < layout.length; i += 2)
			{
				String name = (String)stringType.make(layout[i], null, null);
				HGHandle typeHandle = hg.refreshHandle(layout[i+1]);
				type.addProjection(new HGAbstractCompositeType.Projection(name, typeHandle));
			}
			return type;
		}
	}

	public HGPersistentHandle store(Object instance) 
	{
		if (! (instance instanceof HGAbstractType))
			throw new HGException("Attempt to store an abstract type, which is not an instance of HGAbstractType");
		HGPersistentHandle pHandle = HGHandleFactory.makeHandle();
		if (! (instance instanceof HGCompositeType))
			hg.getStore().store(pHandle, new HGPersistentHandle[] { HGHandleFactory.nullHandle()});
		else
		{
			HGCompositeType composite = (HGCompositeType)instance;
			HGAtomType stringType = hg.getTypeSystem().getAtomType(String.class);
			ArrayList<HGPersistentHandle> layout = new ArrayList<HGPersistentHandle>();
			for (Iterator<String> i = composite.getDimensionNames(); i.hasNext(); )
			{
				String name = i.next();
				layout.add(stringType.store(name));
				layout.add( 
					hg.getPersistentHandle(hg.getPersistentHandle(composite.getProjection(name).getType())));				
			}
			hg.getStore().store(pHandle, layout.toArray(new HGPersistentHandle[layout.size()]));
		}
		return pHandle;
	}

	public void release(HGPersistentHandle handle) 
	{
		hg.getStore().remove(handle);
	}

	public boolean subsumes(Object general, Object specific) 
	{
		return false;
	}
}