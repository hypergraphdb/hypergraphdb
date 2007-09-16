/*
 * This file is part of the HyperGraphDB source distribution. This is copyrighted
 * software. For permitted uses, licensing options and redistribution, please see 
 * the LicensingInformation file at the root level of the distribution. 
 *
 * Copyright (c) 2005-2006
 *  Kobrix Software, Inc.  All rights reserved.
 */
package org.hypergraphdb.type;

import org.hypergraphdb.HGHandle;
import org.hypergraphdb.HGPersistentHandle;
import org.hypergraphdb.LazyRef;

/**
 * <p>
 * A <code>RecordTypeConstructor</code> represents the HG (meta) type of a <code>RecordType</code>.
 * A <code>RecordType</code> is constituted by a definite set of <code>Slot</code>s. It is 
 * itself record and managed within hypergraph through a <code>RecordTypeConstructor</code>.
 * </p>
 * 
 * @author Borislav Iordanov
 */
public class RecordTypeConstructor extends HGAtomTypeBase
{

    public Object make(HGPersistentHandle handle, LazyRef<HGHandle[]> targetSet, LazyRef<HGHandle[]> incidenceSet)
    {
        RecordType result = new RecordType();
        result.setHyperGraph(graph);
        HGPersistentHandle [] layout = graph.getStore().getLink(handle);
        for (int i = 0; i < layout.length; i++)
        {
            result.addSlot(layout[i]);
        }
        return result;
    }

    public HGPersistentHandle store(Object instance)
    {
        RecordType recordType = (RecordType)instance;
        HGPersistentHandle [] layout = new HGPersistentHandle[recordType.slotCount()];
        for (int i = 0; i < layout.length; i++)
        {
        	layout[i] = graph.getPersistentHandle(recordType.getAt(i));
        }
        return graph.getStore().store(layout);
    }

    public void release(HGPersistentHandle handle)
    {
        graph.getStore().removeLink(handle);
    }

}