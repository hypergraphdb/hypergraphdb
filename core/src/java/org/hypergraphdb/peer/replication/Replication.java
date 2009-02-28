package org.hypergraphdb.peer.replication;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.query.AnyAtomCondition;
import org.hypergraphdb.query.HGAtomPredicate;

public class Replication
{
    private HGAtomPredicate atomInterests = new AnyAtomCondition();
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