/* 
 * This file is part of the HyperGraphDB source distribution. This is copyrighted 
 * software. For permitted uses, licensing options and redistribution, please see  
 * the LicensingInformation file at the root level of the distribution.  
 * 
 * Copyright (c) 2005-2010 Kobrix Software, Inc.  All rights reserved. 
 */
package org.hypergraphdb.peer.bootstrap;

import java.util.Map;

import org.hypergraphdb.peer.BootstrapPeer;
import org.hypergraphdb.peer.HGDBOntology;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Performative;
import org.hypergraphdb.peer.replication.CatchUpTaskServer;
import org.hypergraphdb.peer.replication.GetInterestsTask;
import org.hypergraphdb.peer.replication.PublishInterestsTask;
import org.hypergraphdb.peer.replication.RememberTaskServer;
import org.hypergraphdb.peer.replication.Replication;
import org.hypergraphdb.peer.workflow.QueryTaskServer;

public class ReplicationBootstrap implements BootstrapPeer
{
	public void bootstrap(HyperGraphPeer peer, Map<String, Object> config)
	{
        peer.getActivityManager().registerActivityType(PublishInterestsTask.class);	    
/*		peer.getPeerInterface().registerTaskFactory(Performative.CallForProposal, 
												    HGDBOntology.REMEMBER_ACTION, 
												    new RememberTaskServer.RememberTaskServerFactory());
		peer.getPeerInterface().registerTaskFactory(Performative.Request, 
												    HGDBOntology.ATOM_INTEREST, 
												    new PublishInterestsTask.PublishInterestsFactory());
		peer.getPeerInterface().registerTaskFactory(Performative.Request, 
													HGDBOntology.QUERY, 
													new QueryTaskServer.QueryTaskFactory());
		peer.getPeerInterface().registerTaskFactory(Performative.Request, 
													HGDBOntology.CATCHUP, 
													new CatchUpTaskServer.CatchUpTaskServerFactory());
		peer.getPeerInterface().registerTaskFactory(Performative.Inform, 
													HGDBOntology.ATOM_INTEREST, 
													new GetInterestsTask.GetInterestsFactory()); */

	    Replication replication = new Replication(peer);
	    peer.getObjectContext().put(Replication.class.getName(), replication);
	    // TODO: read atom interests from configuration....	   
    	replication.catchUp();          				
	}
}
