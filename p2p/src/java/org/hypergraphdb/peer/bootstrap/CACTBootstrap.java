/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.bootstrap;

import mjson.Json;
import org.hypergraphdb.peer.BootstrapPeer;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.cact.AddAtom;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.hypergraphdb.peer.cact.GetAtom;
import org.hypergraphdb.peer.cact.GetAtomType;
import org.hypergraphdb.peer.cact.GetClassForType;
import org.hypergraphdb.peer.cact.QueryCount;
import org.hypergraphdb.peer.cact.RemoveAtom;
import org.hypergraphdb.peer.cact.ReplaceAtom;
import org.hypergraphdb.peer.cact.RemoteQueryExecution;
import org.hypergraphdb.peer.cact.RunRemoteQuery;
import org.hypergraphdb.peer.cact.TransferGraph;

public class CACTBootstrap implements BootstrapPeer
{
    public void bootstrap(HyperGraphPeer peer, Json config)
    {
        peer.getActivityManager().registerActivityType(GetClassForType.TYPENAME, GetClassForType.class);
        peer.getActivityManager().registerActivityType(DefineAtom.TYPENAME, DefineAtom.class);
        peer.getActivityManager().registerActivityType(TransferGraph.TYPENAME, TransferGraph.class);
        peer.getActivityManager().registerActivityType(GetAtom.TYPENAME, GetAtom.class);
        peer.getActivityManager().registerActivityType(AddAtom.TYPENAME, AddAtom.class);
        peer.getActivityManager().registerActivityType(GetAtomType.TYPENAME, GetAtomType.class);
        peer.getActivityManager().registerActivityType(RemoveAtom.TYPENAME, RemoveAtom.class);
        peer.getActivityManager().registerActivityType(ReplaceAtom.TYPENAME, ReplaceAtom.class);
        peer.getActivityManager().registerActivityType(RunRemoteQuery.TYPENAME, RunRemoteQuery.class);
        peer.getActivityManager().registerActivityType(QueryCount.TYPENAME, QueryCount.class);
        
        RemoteQueryExecution.ResultSetOpen.getConst();
        
        peer.getActivityManager().registerActivityType(RemoteQueryExecution.TYPENAME, 
                                                       RemoteQueryExecution.class);        
        peer.getActivityManager().registerActivityType(RemoteQueryExecution.IterateActivity.TYPENAME, 
                                                       RemoteQueryExecution.IterateActivity.class);
    }
}
