package org.hypergraphdb.peer.workflow;

import java.util.UUID;

import org.hypergraphdb.peer.HyperGraphPeer;
import org.hypergraphdb.peer.Message;

/**
 * <p>
 * An activity implementation based on a finite state machine defined through
 * method annotations.  
 * </p>
 * 
 * @author Borislav Iordanov
 *
 */
public abstract class FSMActivity extends Activity
{
    public FSMActivity(HyperGraphPeer thisPeer)
    {
        super(thisPeer);
    }
    
    public FSMActivity(HyperGraphPeer thisPeer, UUID id)
    {
        super(thisPeer, id);
    }
    
    /**
     * <p>Empty method - override to implement activity initiation.</p>
     */
    public void initiate() { }
    
    /**
     * <p>Empty method - can't override because message handling for
     * <code>FSMActivity</code> is automated by the framework.</p>
     */
    public final void handleMessage(Message message) { }
}