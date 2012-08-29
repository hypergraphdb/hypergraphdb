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
import org.hypergraphdb.peer.workflow.AffirmIdentity;

public class AffirmIdentityBootstrap implements BootstrapPeer
{

    public void bootstrap(HyperGraphPeer peer, Json config)
    {
        peer.getActivityManager().registerActivityType(AffirmIdentity.TYPE_NAME, 
                                                       AffirmIdentity.class);
//        AffirmIdentityTask.Factory factory = new AffirmIdentityTask.Factory();
/*        peer.getPeerInterface().registerTaskFactory(Performative.Inform, 
                                                    HGDBOntology.AFFIRM_IDENTITY, 
                                                    factory); */
//        factory.newTask(peer, null).run();
    }
}
