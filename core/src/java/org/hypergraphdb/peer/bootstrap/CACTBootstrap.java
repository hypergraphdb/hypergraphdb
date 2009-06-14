package org.hypergraphdb.peer.bootstrap;

import java.util.Map;

import org.hypergraphdb.peer.BootstrapPeer;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.hypergraphdb.peer.cact.GetClassForType;
import org.hypergraphdb.peer.cact.TransferGraph;

public class CACTBootstrap implements BootstrapPeer
{
    public void bootstrap(HyperGraphPeer peer, Map<String, Object> config)
    {
        peer.getActivityManager().registerActivityType(GetClassForType.TYPENAME, GetClassForType.class);
        peer.getActivityManager().registerActivityType(DefineAtom.TYPENAME, DefineAtom.class);
        peer.getActivityManager().registerActivityType(TransferGraph.TYPENAME, TransferGraph.class);
    }
}