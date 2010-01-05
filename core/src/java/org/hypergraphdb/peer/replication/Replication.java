/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.replication;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.hypergraphdb.peer.HGPeerIdentity;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.query.HGAtomPredicate;

public class Replication
{
    private HGAtomPredicate atomInterests = new AnyAtomCondition();
    private Map<HGPeerIdentity, HGAtomPredicate> othersInterests = 
        Collections.synchronizedMap(new HashMap<HGPeerIdentity, HGAtomPredicate>());
    
    private HyperGraphPeer thisPeer;
    
    public Replication(HyperGraphPeer thisPeer)
    {
        this.thisPeer = thisPeer;        
    }
    
    public static Replication get(HyperGraphPeer peer)
    {
        return (Replication)peer.getObjectContext().get(Replication.class.getName());
    }
    
    public HGAtomPredicate getAtomInterests()
    {
        return atomInterests;
    }

    public void setAtomInterests(HGAtomPredicate atomInterests)
    {
        this.atomInterests = atomInterests;
    }    

    public Map<HGPeerIdentity, HGAtomPredicate> getOthersInterests()
    {
        return othersInterests;
    }
    
    /**
     * Initializes a catch-up phase. During this all the known peers will be connected to see if any information has been sent to this peer 
     * while it was off line. If there is any, the peer should not resume normal operations until this task completes. 
     */
    public void catchUp()
    {
        CatchUpTaskClient catchUpTask = new CatchUpTaskClient(thisPeer, null);
        catchUpTask.run();
    }    
}
