package org.hypergraphdb.peer.bootstrap;

import java.util.Map;

import org.hypergraphdb.peer.BootstrapPeer;
import org.hypergraphdb.peer.HGDBOntology;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.protocol.Performative;
import org.hypergraphdb.peer.workflow.AffirmIdentityTask;

public class AffirmIdentityBootstrap implements BootstrapPeer
{

    public void bootstrap(HyperGraphPeer peer, Map<String, Object> config)
    {
        AffirmIdentityTask.Factory factory = new AffirmIdentityTask.Factory();
        peer.getPeerInterface().registerTaskFactory(Performative.Inform, 
                                                    HGDBOntology.AFFIRM_IDENTITY, 
                                                    factory);
//        factory.newTask(peer, null).run();
    }
}