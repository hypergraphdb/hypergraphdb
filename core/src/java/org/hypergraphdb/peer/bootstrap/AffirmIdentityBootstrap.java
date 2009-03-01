package org.hypergraphdb.peer.bootstrap;

import java.util.Map;

import org.hypergraphdb.peer.BootstrapPeer;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.workflow.AffirmIdentity;

public class AffirmIdentityBootstrap implements BootstrapPeer
{

    public void bootstrap(HyperGraphPeer peer, Map<String, Object> config)
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