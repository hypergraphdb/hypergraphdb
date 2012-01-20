/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.cact;

import java.util.UUID;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.workflow.FSMActivity;

/**
 * <p>
 * This activity can be used to synchronize types between two peers.
 * 
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public class SyncTypes extends FSMActivity
{
    public SyncTypes(HyperGraphPeer thisPeer)
    {
        super(thisPeer);
    }
    
    public SyncTypes(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }

    @Override
    public void initiate()
    {
        super.initiate();
    }
}
