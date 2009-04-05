package org.hypergraphdb.peer.bootstrap;

import java.util.Map;

import org.hypergraphdb.peer.BootstrapPeer;
import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.cact.DefineAtom;
import org.hypergraphdb.peer.cact.GetClassForType;

public class CACTBootstrap implements BootstrapPeer
{
    public void bootstrap(HyperGraphPeer peer, Map<String, Object> config)
    {
        peer.getActivityManager().registerActivityType(GetClassForType.TYPENAME, GetClassForType.class);
        peer.getActivityManager().registerActivityType(DefineAtom.TYPENAME, DefineAtom.class);
    }
}