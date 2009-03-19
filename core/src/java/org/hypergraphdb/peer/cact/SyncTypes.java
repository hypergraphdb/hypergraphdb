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